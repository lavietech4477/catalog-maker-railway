import base64
import io
import json
import os
import queue
import shutil
import tempfile
import threading
import time
import uuid
from datetime import datetime, timedelta
from functools import lru_cache

import dropbox
import pandas as pd
import pyxlsb
from flask import Flask, Response, jsonify, render_template, request, send_file, send_from_directory, stream_with_context
from fpdf import FPDF
from PIL import Image, ImageOps
from werkzeug.exceptions import RequestEntityTooLarge


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PUBLIC_DIR = os.path.join(BASE_DIR, "public")
LOGO_PATH = os.path.join(BASE_DIR, "data", "bagzonelogo.jpg")
GS1_FILE = os.path.join(BASE_DIR, "data", "GS1.xlsb")


def load_local_env():
    env_path = os.path.join(BASE_DIR, ".env.local")
    if not os.path.isfile(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().lstrip("\ufeff")
            value = value.strip().strip('"').strip("'")
            if not os.environ.get(key):
                os.environ[key] = value


load_local_env()

GENERATED_DIR = os.path.abspath(os.getenv("GENERATED_DIR", os.path.join(BASE_DIR, "generated")))
JOBS_DIR = os.path.join(GENERATED_DIR, "jobs")
DROPBOX_FOLDER = os.getenv("DROPBOX_FOLDER", "/Catelogue Trail1")
SHEET_NAME = "Master"
HEADER_ROW_INDEX = 1
SKU_COLUMN_NAME = "Article"
ALLOWED_EXTENSIONS = {".csv", ".xlsx"}
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_MINUTES", "180")) * 60
MAX_PARALLEL_JOBS = max(1, int(os.getenv("MAX_PARALLEL_JOBS", "2")))
DROPBOX_TIMEOUT_SECONDS = max(5, int(os.getenv("DROPBOX_TIMEOUT_SECONDS", "20")))

CATALOG_FIELDS = [
    ("BRAND", "Brand"),
    ("Article", "Article (SKU)"),
    ("EAN/UPC", "EAN / UPC"),
    ("Article description in SAP", "Title"),
    ("Colour", "Colour"),
    ("Size", "Size"),
    ("Stock", "Stock"),
    ("NEW MRP", "MRP"),
]


app = Flask(__name__, static_folder=None)
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_UPLOAD_MB", "20")) * 1024 * 1024

jobs = {}
jobs_lock = threading.Lock()
job_queue = queue.Queue()
workers_started = False


def clean_value(value):
    if value is None:
        return ""
    if isinstance(value, float):
        if value == int(value):
            return str(int(value))
        return str(round(value, 6))
    if isinstance(value, str):
        return value.strip()
    return str(value)


def excel_date_to_str(value):
    if isinstance(value, (int, float)):
        try:
            return (datetime(1899, 12, 30) + timedelta(days=value)).strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            return str(value)
    return str(value)


@lru_cache(maxsize=1)
def load_gs1_master(filepath):
    if not os.path.isfile(filepath):
        raise FileNotFoundError("GS1 master file is missing.")

    wb = pyxlsb.open_workbook(filepath)
    with wb.get_sheet(SHEET_NAME) as sheet:
        all_rows = list(sheet.rows())

    raw_headers = [cell.v for cell in all_rows[HEADER_ROW_INDEX]]
    headers = []
    seen = {}
    for idx, header in enumerate(raw_headers):
        if header is None or (isinstance(header, str) and not header.strip()):
            name = f"Column_{idx}"
        else:
            name = str(header).strip()
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        headers.append(name)

    try:
        sku_col_idx = headers.index(SKU_COLUMN_NAME)
    except ValueError as exc:
        raise ValueError(f"Column '{SKU_COLUMN_NAME}' not found in GS1 headers.") from exc

    sku_lookup = {}
    for row in all_rows[HEADER_ROW_INDEX + 1 :]:
        cells = [cell.v for cell in row]
        sku_val = cells[sku_col_idx] if sku_col_idx < len(cells) else None
        if sku_val is None:
            continue

        sku_key = str(sku_val).strip().upper()
        if not sku_key:
            continue

        row_dict = {}
        for idx, header in enumerate(headers):
            row_dict[header] = clean_value(cells[idx] if idx < len(cells) else None)
        sku_lookup.setdefault(sku_key, []).append(row_dict)

    return sku_lookup, headers


def get_dropbox_client():
    app_key = os.getenv("DROPBOX_APP_KEY", "")
    app_secret = os.getenv("DROPBOX_APP_SECRET", "")
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN", "")
    access_token = os.getenv("DROPBOX_ACCESS_TOKEN", "")

    if refresh_token and app_key and app_secret:
        return dropbox.Dropbox(
            oauth2_access_token=access_token or None,
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
            timeout=DROPBOX_TIMEOUT_SECONDS,
        )
    if access_token:
        return dropbox.Dropbox(access_token, timeout=DROPBOX_TIMEOUT_SECONDS)
    raise RuntimeError("Dropbox credentials are not configured.")


def prepare_image_for_pdf(image_path):
    optimized_path = f"{os.path.splitext(image_path)[0]}_pdf.jpg"
    try:
        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img)
            if img.mode in ("RGBA", "LA"):
                background = Image.new("RGB", img.size, (255, 255, 255))
                background.paste(img, mask=img.getchannel("A"))
                img = background
            else:
                img = img.convert("RGB")

            img.thumbnail((900, 900), Image.Resampling.LANCZOS)
            img.save(optimized_path, "JPEG", quality=72, optimize=True, progressive=True)
        return optimized_path
    except Exception:
        return image_path


def search_and_download_sku_images(dbx, sku, temp_dir):
    matches = []
    try:
        result = dbx.files_search_v2(
            query=sku,
            options=dropbox.files.SearchOptions(
                path=DROPBOX_FOLDER,
                max_results=20,
                file_extensions=["jpg", "jpeg", "png", "webp"],
            ),
        )
        for match in result.matches:
            metadata = match.metadata.get_metadata()
            if isinstance(metadata, dropbox.files.FileMetadata):
                name_no_ext = os.path.splitext(metadata.name)[0].lower()
                if sku.lower() in name_no_ext:
                    matches.append({"name": metadata.name, "path": metadata.path_display})
    except Exception:
        return []

    matches.sort(key=lambda item: item["name"])
    downloaded = []
    for file_info in matches:
        try:
            _, response = dbx.files_download(file_info["path"])
            local_path = os.path.join(temp_dir, file_info["name"])
            with open(local_path, "wb") as fp:
                fp.write(response.content)
            downloaded.append(prepare_image_for_pdf(local_path))
        except Exception:
            continue

    return downloaded


class CatalogPDF(FPDF):
    def __init__(self, logo_path, category_title="CATALOG"):
        super().__init__(orientation="P", unit="pt", format="A4")
        self.logo_path = logo_path
        self.category_title = category_title
        self.set_auto_page_break(auto=False)

    def build_header(self):
        if os.path.isfile(self.logo_path):
            self.image(self.logo_path, x=39, y=42, w=55)

        self.set_font("Helvetica", "B", 8)
        self.set_text_color(60, 60, 60)
        self.set_xy(460, 63)
        self.cell(100, 10, f"Date: {datetime.now().strftime('%d/%m/%Y')}", align="R")

        self.set_font("Helvetica", "B", 10)
        self.set_text_color(0, 0, 0)
        self.set_xy(0, 94)
        self.cell(self.w, 12, self.category_title, align="C")

        self.set_draw_color(255, 204, 0)
        self.set_line_width(2)
        self.line(39, 115, self.w - 39, 115)

    def draw_main_image(self, image_path):
        box_x, box_y = 42, 130
        box_w, box_h = 253, 253

        self.set_draw_color(0, 0, 0)
        self.set_line_width(1.2)
        self.rect(box_x, box_y, box_w, box_h)

        if image_path and os.path.isfile(image_path):
            try:
                with Image.open(image_path) as img:
                    img_w, img_h = img.size
                pad = 8
                avail_w = box_w - 2 * pad
                avail_h = box_h - 2 * pad
                scale = min(avail_w / img_w, avail_h / img_h)
                draw_w = img_w * scale
                draw_h = img_h * scale
                draw_x = box_x + pad + (avail_w - draw_w) / 2
                draw_y = box_y + pad + (avail_h - draw_h) / 2
                self.image(image_path, x=draw_x, y=draw_y, w=draw_w, h=draw_h)
                return
            except Exception:
                pass

        self.set_xy(box_x + 10, box_y + box_h / 2)
        self.set_font("Helvetica", "I", 9)
        self.set_text_color(120, 120, 120)
        self.cell(box_w - 20, 10, "No image available", align="C")
        self.set_text_color(0, 0, 0)

    def draw_info_fields(self, sku_info):
        start_x = 344
        start_y = 200
        line_gap = 22

        for idx, (gs1_key, display_label) in enumerate(CATALOG_FIELDS):
            y_pos = start_y + idx * line_gap
            value = sku_info.get(gs1_key, "N/A") or "N/A"

            self.set_font("Helvetica", "B", 9)
            self.set_text_color(0, 0, 0)
            self.set_xy(start_x, y_pos)
            label_text = f"{display_label}:  "
            self.cell(self.get_string_width(label_text), 10, label_text)

            self.set_font("Helvetica", "", 9)
            self.set_text_color(50, 50, 50)
            val_str = str(value)
            max_w = self.w - 45 - start_x - self.get_string_width(label_text)
            while self.get_string_width(val_str) > max_w and len(val_str) > 3:
                val_str = val_str[:-1]
            self.cell(0, 10, val_str)

    def draw_thumbnails(self, image_paths):
        if not image_paths or len(image_paths) < 2:
            return

        thumbs = image_paths[1:]
        grid_x_start = 42
        grid_y_start = 420
        thumb_size = 86
        gap = 13.75
        per_row = 5

        for idx, img_path in enumerate(thumbs):
            col = idx % per_row
            row = idx // per_row
            x_pos = grid_x_start + col * (thumb_size + gap)
            y_pos = grid_y_start + row * (thumb_size + gap + 20)

            self.set_draw_color(0, 0, 0)
            self.set_line_width(0.8)
            self.rect(x_pos, y_pos, thumb_size, thumb_size)

            if os.path.isfile(img_path):
                try:
                    with Image.open(img_path) as img:
                        img_w, img_h = img.size
                    pad = 4
                    avail = thumb_size - 2 * pad
                    scale = min(avail / img_w, avail / img_h)
                    draw_w = img_w * scale
                    draw_h = img_h * scale
                    draw_x = x_pos + pad + (avail - draw_w) / 2
                    draw_y = y_pos + pad + (avail - draw_h) / 2
                    self.image(img_path, x=draw_x, y=draw_y, w=draw_w, h=draw_h)
                except Exception:
                    continue

    def draw_footer(self):
        self.set_font("Helvetica", "", 6.5)
        self.set_text_color(80, 80, 80)
        self.set_xy(39, 650)
        self.cell(0, 10, "Propriety: Bagzone Lifestyle Pvt. Ltd.")

        self.set_draw_color(255, 204, 0)
        self.set_line_width(1.2)
        self.line(39, 665, self.w - 39, 665)

    def add_sku_page(self, sku_info, image_paths):
        self.add_page()
        self.build_header()
        self.draw_main_image(image_paths[0] if image_paths else None)
        self.draw_info_fields(sku_info)
        self.draw_thumbnails(image_paths)
        self.draw_footer()


def parse_uploaded_file(uploaded_file):
    filename = uploaded_file.filename or ""
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise ValueError("Only CSV and XLSX files are supported.")

    if ext == ".csv":
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file, engine="openpyxl")

    if df.empty:
        raise ValueError("Uploaded file has no rows.")

    sku_col = next(
        (candidate for candidate in ["SKU", "sku", "Article", "article", "ARTICLE", "Item Id", "item_id", "ItemId"] if candidate in df.columns),
        df.columns[0],
    )
    stock_col = next(
        (candidate for candidate in ["Stock", "stock", "STOCK", "Qty", "qty", "QTY", "Quantity"] if candidate in df.columns),
        df.columns[1] if len(df.columns) >= 2 else None,
    )

    sku_list = df[sku_col].dropna().astype(str).str.strip().tolist()
    sku_list = [sku for sku in sku_list if sku]
    if not sku_list:
        raise ValueError("No SKU values found in the uploaded file.")

    stock_lookup = {}
    if stock_col is not None:
        for _, row in df.iterrows():
            sku_val = str(row[sku_col]).strip().upper()
            if not sku_val or sku_val == "NAN":
                continue
            stock_val = row.get(stock_col, "N/A")
            if pd.notna(stock_val):
                stock_lookup[sku_val] = str(int(stock_val)) if isinstance(stock_val, float) and stock_val == int(stock_val) else str(stock_val)
            else:
                stock_lookup[sku_val] = "N/A"

    return df, sku_list, stock_lookup


def pdf_to_bytes(pdf):
    output = pdf.output(dest="S")
    if isinstance(output, str):
        return output.encode("latin-1")
    return bytes(output)


def stream_event(payload):
    return json.dumps(payload, separators=(",", ":")) + "\n"


def ensure_job_dirs():
    os.makedirs(JOBS_DIR, exist_ok=True)


def job_state_path(job_id):
    return os.path.join(JOBS_DIR, f"{job_id}.json")


def job_payload_path(job_id):
    return os.path.join(JOBS_DIR, f"{job_id}.payload.json")


def write_json_file(path, payload):
    ensure_job_dirs()
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, separators=(",", ":"))
    os.replace(temp_path, path)


def read_json_file(path):
    with open(path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def save_job_state(job):
    write_json_file(job_state_path(job["id"]), job)


def load_job_state(job_id):
    path = job_state_path(job_id)
    if not os.path.isfile(path):
        return None
    try:
        return read_json_file(path)
    except (OSError, json.JSONDecodeError):
        return None


def load_job_payload(job_id):
    return read_json_file(job_payload_path(job_id))


def cleanup_old_jobs():
    now = time.time()
    stale_job_ids = []
    stale_files = []
    with jobs_lock:
        for job_id, job in jobs.items():
            if now - job.get("created_at", now) > JOB_TTL_SECONDS:
                stale_job_ids.append(job_id)
                if job.get("file_path"):
                    stale_files.append(job["file_path"])

        for job_id in stale_job_ids:
            jobs.pop(job_id, None)

    ensure_job_dirs()
    for file_name in os.listdir(JOBS_DIR):
        if not file_name.endswith(".json") or file_name.endswith(".payload.json"):
            continue
        job_id = file_name[:-5]
        job = load_job_state(job_id)
        if not job:
            continue
        if now - job.get("created_at", now) > JOB_TTL_SECONDS:
            stale_job_ids.append(job_id)
            if job.get("file_path"):
                stale_files.append(job["file_path"])

    for job_id in set(stale_job_ids):
        for path in (job_state_path(job_id), job_payload_path(job_id)):
            try:
                os.remove(path)
            except OSError:
                pass

    for file_path in stale_files:
        try:
            os.remove(file_path)
        except OSError:
            pass


def update_job(job_id, **changes):
    with jobs_lock:
        job = jobs.get(job_id) or load_job_state(job_id)
        if not job:
            return
        job.update(changes)
        job["updated_at"] = time.time()
        jobs[job_id] = job
        save_job_state(job)


def public_job(job):
    payload = {
        "id": job["id"],
        "status": job["status"],
        "message": job.get("message", ""),
        "current": job.get("current", 0),
        "total": job.get("total", 0),
        "percent": job.get("percent", 0),
        "sku": job.get("sku", ""),
        "fileName": job.get("file_name", ""),
        "summary": job.get("summary"),
        "rows": job.get("rows", []),
        "error": job.get("error", ""),
        "stopRequested": bool(job.get("stop_requested", False)),
        "isPartial": bool(job.get("partial", False)),
    }
    if job["status"] in {"complete", "partial_complete"}:
        payload["downloadUrl"] = f"/api/jobs/{job['id']}/download"
    return payload


def get_job(job_id):
    with jobs_lock:
        job = jobs.get(job_id) or load_job_state(job_id)
        if job:
            jobs[job_id] = job
            return dict(job)
        return None


def is_stop_requested(job_id):
    job = get_job(job_id)
    return bool(job and job.get("stop_requested"))


def finalize_catalog_job(job_id, pdf, results_summary, total_skus, partial=False):
    completed_pages = len(results_summary)
    if partial and completed_pages == 0:
        update_job(
            job_id,
            status="cancelled",
            message="Catalog generation stopped before any pages were created.",
            current=0,
            percent=0,
            summary={
                "pages": 0,
                "withProductData": 0,
                "imagesFetched": 0,
                "missingImages": 0,
            },
            rows=[],
            partial=True,
        )
        return

    os.makedirs(GENERATED_DIR, exist_ok=True)
    suffix = "Partial_Catalog" if partial else "Catalog"
    file_name = f"{suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{job_id[:8]}.pdf"
    file_path = os.path.join(GENERATED_DIR, file_name)
    pdf.output(file_path)

    total_imgs = sum(item["images"] for item in results_summary)
    total_gs1 = sum(1 for item in results_summary if item["gs1"] == "Found")
    missing_images = sum(1 for item in results_summary if item["images"] == 0)
    summary = {
        "pages": completed_pages,
        "withProductData": total_gs1,
        "imagesFetched": total_imgs,
        "missingImages": missing_images,
    }

    update_job(
        job_id,
        status="partial_complete" if partial else "complete",
        message="Partial catalog ready." if partial else "Catalog ready.",
        current=completed_pages,
        percent=round((completed_pages / total_skus) * 100) if total_skus else 0,
        summary=summary,
        rows=results_summary[-500:],
        file_name=file_name,
        file_path=file_path,
        partial=partial,
    )


def create_job(sku_list, stock_lookup):
    cleanup_old_jobs()
    job_id = uuid.uuid4().hex
    job = {
        "id": job_id,
        "status": "queued",
        "message": "Catalog job queued.",
        "current": 0,
        "total": len(sku_list),
        "percent": 0,
        "sku": "",
        "summary": None,
        "rows": [],
        "file_name": "",
        "file_path": "",
        "error": "",
        "stop_requested": False,
        "partial": False,
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    with jobs_lock:
        jobs[job_id] = job
        save_job_state(job)
        write_json_file(job_payload_path(job_id), {"sku_list": sku_list, "stock_lookup": stock_lookup})
    job_queue.put({"job_id": job_id})
    return job


def generate_catalog_file(job_id, sku_list=None, stock_lookup=None):
    if sku_list is None or stock_lookup is None:
        payload = load_job_payload(job_id)
        sku_list = payload["sku_list"]
        stock_lookup = payload["stock_lookup"]

    update_job(job_id, status="running", message="Loading GS1 data.", total=len(sku_list))
    sku_lookup, _headers = load_gs1_master(GS1_FILE)

    update_job(job_id, message="Connecting to Dropbox image server.")
    dbx = get_dropbox_client()
    dbx.users_get_current_account()

    results_summary = []
    pdf = CatalogPDF(logo_path=LOGO_PATH, category_title="CATALOG")
    temp_dir = tempfile.mkdtemp()

    try:
        for index, sku in enumerate(sku_list, start=1):
            if is_stop_requested(job_id):
                update_job(job_id, status="stopping", message="Stopping catalog generation and creating partial PDF.")
                break

            sku_upper = sku.strip().upper()
            update_job(
                job_id,
                current=index,
                sku=sku_upper,
                percent=round(((index - 1) / len(sku_list)) * 100),
                message=f"Processing {index} of {len(sku_list)}: {sku_upper}",
            )

            matches = sku_lookup.get(sku_upper, [])
            if matches:
                info = dict(matches[0])
                gs1_status = "Found"
            else:
                info = {key: "N/A" for key, _label in CATALOG_FIELDS}
                info["Article"] = sku_upper
                gs1_status = "Missing"

            info["Stock"] = stock_lookup.get(sku_upper, "N/A")
            images = search_and_download_sku_images(dbx, sku_upper, temp_dir)
            pdf.add_sku_page(info, images)

            results_summary.append(
                {
                    "sku": sku_upper,
                    "brand": info.get("BRAND", "N/A"),
                    "title": info.get("Article description in SAP", "N/A"),
                    "newMrp": info.get("NEW MRP", "N/A"),
                    "images": len(images),
                    "gs1": gs1_status,
                    "dropbox": "Found" if images else "No images",
                }
            )

            update_job(
                job_id,
                current=index,
                rows=results_summary[-500:],
                percent=round((index / len(sku_list)) * 100),
                message=f"Completed {index} of {len(sku_list)} SKUs.",
            )

            if is_stop_requested(job_id):
                update_job(job_id, status="stopping", message="Stopping catalog generation and creating partial PDF.")
                break

        partial = is_stop_requested(job_id)
        update_job(
            job_id,
            message="Creating partial PDF file." if partial else "Creating PDF file.",
            percent=round((len(results_summary) / len(sku_list)) * 100) if partial and sku_list else 98,
        )
        finalize_catalog_job(job_id, pdf, results_summary, len(sku_list), partial=partial)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def worker_loop():
    while True:
        payload = job_queue.get()
        job_id = payload["job_id"]
        try:
            generate_catalog_file(job_id, payload.get("sku_list"), payload.get("stock_lookup"))
        except ValueError as exc:
            update_job(job_id, status="failed", error=str(exc), message="Catalog generation failed.")
        except FileNotFoundError:
            update_job(job_id, status="failed", error="GS1 master file is missing on the server.", message="Catalog generation failed.")
        except Exception as exc:
            update_job(job_id, status="failed", error=f"Catalog generation failed: {exc}", message="Catalog generation failed.")
        finally:
            job_queue.task_done()


def requeue_unfinished_jobs():
    ensure_job_dirs()
    for file_name in os.listdir(JOBS_DIR):
        if not file_name.endswith(".json") or file_name.endswith(".payload.json"):
            continue
        job_id = file_name[:-5]
        job = load_job_state(job_id)
        if not job or job.get("status") not in {"queued", "running", "stopping"}:
            continue
        if not os.path.isfile(job_payload_path(job_id)):
            update_job(job_id, status="failed", error="Catalog job payload is missing.", message="Catalog generation failed.")
            continue
        update_job(job_id, status="queued", message="Catalog job resumed after server restart.")
        job_queue.put({"job_id": job_id})


def start_job_workers():
    global workers_started
    if workers_started:
        return
    workers_started = True
    requeue_unfinished_jobs()
    for index in range(MAX_PARALLEL_JOBS):
        worker = threading.Thread(target=worker_loop, name=f"catalog-worker-{index + 1}", daemon=True)
        worker.start()


@app.errorhandler(RequestEntityTooLarge)
def handle_file_too_large(_error):
    return jsonify({"success": False, "error": "Uploaded file is too large."}), 413


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/assets/<path:filename>")
def public_assets(filename):
    return send_from_directory(os.path.join(PUBLIC_DIR, "assets"), filename)


@app.get("/sample-template")
def sample_template():
    sample_df = pd.DataFrame({"SKU": pd.Series(dtype="str"), "Stock": pd.Series(dtype="int")})
    sample_buffer = io.BytesIO()
    sample_df.to_excel(sample_buffer, index=False, engine="openpyxl")
    sample_buffer.seek(0)
    return send_file(
        sample_buffer,
        as_attachment=True,
        download_name="SKU_Template.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.post("/api/jobs")
def start_catalog_job():
    uploaded_file = request.files.get("sku_file")
    if uploaded_file is None or not uploaded_file.filename:
        return jsonify({"success": False, "error": "Please upload a CSV or XLSX file."}), 400

    try:
        _df, sku_list, stock_lookup = parse_uploaded_file(uploaded_file)
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400

    job = create_job(sku_list, stock_lookup)
    return jsonify({"success": True, "job": public_job(job)}), 202


@app.post("/api/jobs/<job_id>/stop")
def stop_catalog_job(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Catalog job was not found or has expired."}), 404
    if job["status"] in {"complete", "partial_complete", "failed", "cancelled"}:
        return jsonify({"success": True, "job": public_job(job)})
    if job["status"] not in {"queued", "running", "stopping"}:
        return jsonify({"success": False, "error": "Catalog job cannot be stopped."}), 409

    update_job(
        job_id,
        status="stopping",
        stop_requested=True,
        message="Stop requested. Creating partial catalog after the current SKU finishes.",
    )
    stopped_job = get_job(job_id)
    return jsonify({"success": True, "job": public_job(stopped_job)})


@app.get("/api/jobs/<job_id>")
def catalog_job_status(job_id):
    cleanup_old_jobs()
    job = get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Catalog job was not found or has expired."}), 404
    return jsonify({"success": True, "job": public_job(job)})


@app.get("/api/jobs/<job_id>/download")
def download_catalog_job(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Catalog job was not found or has expired."}), 404
    if job["status"] not in {"complete", "partial_complete"}:
        return jsonify({"success": False, "error": "Catalog is not ready yet."}), 409
    if not job.get("file_path") or not os.path.isfile(job["file_path"]):
        return jsonify({"success": False, "error": "Generated catalog file is missing."}), 404

    return send_file(
        job["file_path"],
        as_attachment=True,
        download_name=job.get("file_name") or "Catalog.pdf",
        mimetype="application/pdf",
    )


@app.post("/api/generate")
def generate_catalog():
    uploaded_file = request.files.get("sku_file")
    if uploaded_file is None or not uploaded_file.filename:
        return jsonify({"success": False, "error": "Please upload a CSV or XLSX file."}), 400

    try:
        _df, sku_list, stock_lookup = parse_uploaded_file(uploaded_file)
        sku_lookup, _headers = load_gs1_master(GS1_FILE)
        dbx = get_dropbox_client()
        dbx.users_get_current_account()
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400
    except FileNotFoundError:
        return jsonify({"success": False, "error": "GS1 master file is missing on the server."}), 500
    except Exception:
        return jsonify({"success": False, "error": "Unable to start catalog generation. Check server credentials and try again."}), 500

    results_summary = []
    pdf = CatalogPDF(logo_path=LOGO_PATH, category_title="CATALOG")

    temp_dir = tempfile.mkdtemp()
    try:
        for sku in sku_list:
            sku_upper = sku.strip().upper()
            matches = sku_lookup.get(sku_upper, [])
            if matches:
                info = dict(matches[0])
                gs1_status = "Found"
            else:
                info = {key: "N/A" for key, _label in CATALOG_FIELDS}
                info["Article"] = sku_upper
                gs1_status = "Missing"

            info["Stock"] = stock_lookup.get(sku_upper, "N/A")
            images = search_and_download_sku_images(dbx, sku_upper, temp_dir)
            pdf.add_sku_page(info, images)

            results_summary.append(
                {
                    "sku": sku_upper,
                    "brand": info.get("BRAND", "N/A"),
                    "title": info.get("Article description in SAP", "N/A"),
                    "newMrp": info.get("NEW MRP", "N/A"),
                    "images": len(images),
                    "gs1": gs1_status,
                    "dropbox": "Found" if images else "No images",
                }
            )

        pdf_bytes = pdf_to_bytes(pdf)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    file_name = f"Catalog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    total_imgs = sum(item["images"] for item in results_summary)
    total_gs1 = sum(1 for item in results_summary if item["gs1"] == "Found")
    missing_images = sum(1 for item in results_summary if item["images"] == 0)

    return jsonify(
        {
            "success": True,
            "fileName": file_name,
            "pdfBase64": base64.b64encode(pdf_bytes).decode("ascii"),
            "summary": {
                "pages": len(sku_list),
                "withProductData": total_gs1,
                "imagesFetched": total_imgs,
                "missingImages": missing_images,
            },
            "rows": results_summary,
        }
    )


@app.post("/api/generate-stream")
def generate_catalog_stream():
    def event_stream():
        uploaded_file = request.files.get("sku_file")
        if uploaded_file is None or not uploaded_file.filename:
            yield stream_event({"type": "error", "error": "Please upload a CSV or XLSX file."})
            return

        try:
            _df, sku_list, stock_lookup = parse_uploaded_file(uploaded_file)
            yield stream_event({"type": "setup", "message": f"{len(sku_list)} SKUs found. Loading GS1 data.", "total": len(sku_list)})

            sku_lookup, _headers = load_gs1_master(GS1_FILE)
            yield stream_event({"type": "setup", "message": "Connecting to Dropbox image server.", "total": len(sku_list)})

            dbx = get_dropbox_client()
            dbx.users_get_current_account()
        except ValueError as exc:
            yield stream_event({"type": "error", "error": str(exc)})
            return
        except FileNotFoundError:
            yield stream_event({"type": "error", "error": "GS1 master file is missing on the server."})
            return
        except Exception:
            yield stream_event({"type": "error", "error": "Unable to connect to Dropbox. Check server credentials and try again."})
            return

        results_summary = []
        pdf = CatalogPDF(logo_path=LOGO_PATH, category_title="CATALOG")

        temp_dir = tempfile.mkdtemp()
        try:
            for index, sku in enumerate(sku_list, start=1):
                sku_upper = sku.strip().upper()
                yield stream_event(
                    {
                        "type": "progress",
                        "phase": "processing",
                        "current": index,
                        "total": len(sku_list),
                        "sku": sku_upper,
                        "percent": round(((index - 1) / len(sku_list)) * 100),
                        "message": f"Processing {index} of {len(sku_list)}: {sku_upper}",
                    }
                )

                matches = sku_lookup.get(sku_upper, [])
                if matches:
                    info = dict(matches[0])
                    gs1_status = "Found"
                else:
                    info = {key: "N/A" for key, _label in CATALOG_FIELDS}
                    info["Article"] = sku_upper
                    gs1_status = "Missing"

                info["Stock"] = stock_lookup.get(sku_upper, "N/A")
                images = search_and_download_sku_images(dbx, sku_upper, temp_dir)
                pdf.add_sku_page(info, images)

                results_summary.append(
                    {
                        "sku": sku_upper,
                        "brand": info.get("BRAND", "N/A"),
                        "title": info.get("Article description in SAP", "N/A"),
                        "newMrp": info.get("NEW MRP", "N/A"),
                        "images": len(images),
                        "gs1": gs1_status,
                        "dropbox": "Found" if images else "No images",
                    }
                )

                yield stream_event(
                    {
                        "type": "progress",
                        "phase": "done_sku",
                        "current": index,
                        "total": len(sku_list),
                        "sku": sku_upper,
                        "percent": round((index / len(sku_list)) * 100),
                        "images": len(images),
                        "gs1": gs1_status,
                        "message": f"Completed {index} of {len(sku_list)} SKUs.",
                    }
                )

            yield stream_event({"type": "finalizing", "message": "Creating PDF file.", "percent": 98})
            pdf_bytes = pdf_to_bytes(pdf)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

        file_name = f"Catalog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        total_imgs = sum(item["images"] for item in results_summary)
        total_gs1 = sum(1 for item in results_summary if item["gs1"] == "Found")
        missing_images = sum(1 for item in results_summary if item["images"] == 0)

        yield stream_event(
            {
                "type": "complete",
                "success": True,
                "fileName": file_name,
                "pdfBase64": base64.b64encode(pdf_bytes).decode("ascii"),
                "summary": {
                    "pages": len(sku_list),
                    "withProductData": total_gs1,
                    "imagesFetched": total_imgs,
                    "missingImages": missing_images,
                },
                "rows": results_summary,
            }
        )

    return Response(stream_with_context(event_stream()), mimetype="application/x-ndjson")


start_job_workers()


if __name__ == "__main__":
    app.run(debug=True)
