import io
import json
import os
import queue
import re
import shutil
import tempfile
import threading
import time
import uuid
from hashlib import sha1
from datetime import datetime, timedelta
from functools import lru_cache
from numbers import Number

import boto3
import dropbox
import pandas as pd
import pyxlsb
import redis
from sqlalchemy.exc import IntegrityError
from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, send_from_directory, url_for
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from flask_sqlalchemy import SQLAlchemy
from fpdf import FPDF
from PIL import Image, ImageOps
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.security import check_password_hash, generate_password_hash


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
REDIS_URL = os.getenv("REDIS_URL", "")
QUEUE_NAME = os.getenv("QUEUE_NAME", "catalog-jobs")
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(BASE_DIR, 'catalog_app.db')}")
STORAGE_BUCKET = os.getenv("STORAGE_BUCKET", "")
STORAGE_PREFIX = os.getenv("STORAGE_PREFIX", "catalogs").strip("/")
STORAGE_ENDPOINT_URL = os.getenv("STORAGE_ENDPOINT_URL", "")
STORAGE_REGION = os.getenv("STORAGE_REGION", "auto")
ALLOWED_EMAIL_DOMAIN = os.getenv("ALLOWED_EMAIL_DOMAIN", "bagzone.in").strip().lower()
JOB_RETENTION_DAYS = max(1, int(os.getenv("JOB_RETENTION_DAYS", "10")))
IMAGE_CACHE_DAYS = max(1, int(os.getenv("IMAGE_CACHE_DAYS", "30")))
USER_ACTIVITY_LIMIT = max(1, int(os.getenv("USER_ACTIVITY_LIMIT", "5")))
DISPLAY_TIMEZONE_OFFSET_MINUTES = int(os.getenv("DISPLAY_TIMEZONE_OFFSET_MINUTES", "330"))
DISPLAY_TIMEZONE_LABEL = os.getenv("DISPLAY_TIMEZONE_LABEL", "IST").strip()
SHEET_NAME = "Master"
HEADER_ROW_INDEX = 1
SKU_COLUMN_NAME = "Article"
ALLOWED_EXTENSIONS = {".csv", ".xlsx"}
JOB_TTL_SECONDS = JOB_RETENTION_DAYS * 24 * 60 * 60
IMAGE_CACHE_SECONDS = IMAGE_CACHE_DAYS * 24 * 60 * 60
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
    ("RRP", "RRP"),
    ("Customer Discount", "Customer Discount"),
    ("NEW MRP", "MRP"),
]
OPTIONAL_UPLOAD_FIELDS = {"Stock", "RRP", "Customer Discount"}
MISSING_UPLOAD_VALUES = {"", "NA", "N/A", "NAN"}


if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)


app = Flask(__name__, static_folder=None)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-change-me")
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_UPLOAD_MB", "20")) * 1024 * 1024
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = "login"
login_manager.login_message = "Please sign in to use Catalog Maker."

local_job_queue = queue.Queue()
inline_workers_started = False


class User(db.Model, UserMixin):
    __tablename__ = "users"

    id = db.Column(db.String(32), primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    name = db.Column(db.String(255), nullable=False, default="")
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(32), nullable=False, default="user")
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    @property
    def is_active(self):
        return self.active

    @property
    def is_admin(self):
        return self.role == "admin"

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class CatalogJob(db.Model):
    __tablename__ = "catalog_jobs"

    id = db.Column(db.String(32), primary_key=True)
    user_id = db.Column(db.String(32), db.ForeignKey("users.id"), nullable=False, index=True)
    status = db.Column(db.String(32), nullable=False, default="queued", index=True)
    message = db.Column(db.Text, nullable=False, default="")
    current = db.Column(db.Integer, nullable=False, default=0)
    total = db.Column(db.Integer, nullable=False, default=0)
    percent = db.Column(db.Integer, nullable=False, default=0)
    sku = db.Column(db.String(255), nullable=False, default="")
    file_name = db.Column(db.String(255), nullable=False, default="")
    file_path = db.Column(db.Text, nullable=False, default="")
    storage_key = db.Column(db.Text, nullable=False, default="")
    file_blob = db.Column(db.LargeBinary, nullable=True)
    error = db.Column(db.Text, nullable=False, default="")
    stop_requested = db.Column(db.Boolean, nullable=False, default=False)
    partial = db.Column(db.Boolean, nullable=False, default=False)
    summary = db.Column(db.JSON, nullable=True)
    rows = db.Column(db.JSON, nullable=False, default=list)
    payload = db.Column(db.JSON, nullable=False, default=dict)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = db.relationship("User", backref="catalog_jobs")


@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, user_id)


def init_database():
    db.create_all()
    admin_email = os.getenv("ADMIN_EMAIL", "").strip().lower()
    admin_password = os.getenv("ADMIN_PASSWORD", "")
    admin_name = os.getenv("ADMIN_NAME", "Admin").strip() or "Admin"
    if not admin_email or not admin_password:
        return
    if not is_allowed_email(admin_email):
        return

    existing = User.query.filter_by(email=admin_email).first()
    if existing:
        if not existing.is_admin:
            existing.role = "admin"
        if not existing.active:
            existing.active = True
        db.session.commit()
        return

    admin = User(id=uuid.uuid4().hex, email=admin_email, name=admin_name, role="admin", active=True)
    admin.set_password(admin_password)
    db.session.add(admin)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()


def require_admin():
    return current_user.is_authenticated and current_user.is_admin


def normalize_email(email):
    return (email or "").strip().lower()


def is_allowed_email(email):
    if not ALLOWED_EMAIL_DOMAIN:
        return True
    return email.strip().lower().endswith(f"@{ALLOWED_EMAIL_DOMAIN}")


def is_safe_next_url(next_url):
    return bool(next_url) and next_url.startswith("/") and not next_url.startswith("//")


def format_datetime(value):
    if not value:
        return ""
    display_value = value + timedelta(minutes=DISPLAY_TIMEZONE_OFFSET_MINUTES)
    formatted = display_value.strftime("%d %b %Y, %I:%M %p")
    return f"{formatted} {DISPLAY_TIMEZONE_LABEL}" if DISPLAY_TIMEZONE_LABEL else formatted


app.jinja_env.filters["datetime"] = format_datetime


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

def is_missing_upload_value(value):
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        return value.strip().upper() in MISSING_UPLOAD_VALUES
    return False


def format_upload_value(value):
    if is_missing_upload_value(value):
        return ""
    if isinstance(value, float) and value == int(value):
        return str(int(value))
    return str(value).strip()



def format_customer_discount_value(value):
    if is_missing_upload_value(value):
        return ""
    if isinstance(value, Number) and not isinstance(value, bool):
        discount_number = float(value)
        if discount_number and abs(discount_number) < 1:
            discount_number *= 100
        if discount_number == int(discount_number):
            discount_value = str(int(discount_number))
        else:
            discount_value = f"{discount_number:.10f}".rstrip("0").rstrip(".")
    else:
        discount_value = format_upload_value(value)
    discount_value = re.sub(r"\s+", " ", discount_value.replace("%", "")).strip()
    return f"{discount_value} %" if discount_value else ""
    

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


def safe_key_part(value):
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip())
    return text.strip("-") or "item"


def image_cache_prefix(sku):
    prefix = f"{STORAGE_PREFIX}/" if STORAGE_PREFIX else ""
    return f"{prefix}image-cache/{safe_key_part(sku.upper())}/"


def image_cache_root_prefix():
    prefix = f"{STORAGE_PREFIX}/" if STORAGE_PREFIX else ""
    return f"{prefix}image-cache/"


def cached_image_local_path(temp_dir, key, index):
    base_name = os.path.basename(key) or f"image-{index}.jpg"
    return os.path.join(temp_dir, f"cached-{index}-{base_name}")


def load_cached_sku_images(sku, temp_dir):
    s3_client = get_s3_client()
    if not s3_client:
        return []

    try:
        result = s3_client.list_objects_v2(Bucket=STORAGE_BUCKET, Prefix=image_cache_prefix(sku), MaxKeys=50)
    except Exception:
        return []

    objects = sorted(result.get("Contents", []), key=lambda item: item.get("Key", ""))
    fresh_objects = []
    cutoff = datetime.utcnow().timestamp() - IMAGE_CACHE_SECONDS
    for item in objects:
        key = item.get("Key", "")
        if not key or key.endswith("/"):
            continue
        last_modified = item.get("LastModified")
        if last_modified and last_modified.timestamp() < cutoff:
            delete_storage_key(key)
            continue
        fresh_objects.append(item)

    cached_paths = []
    for index, item in enumerate(fresh_objects, start=1):
        key = item["Key"]
        local_path = cached_image_local_path(temp_dir, key, index)
        try:
            s3_client.download_file(STORAGE_BUCKET, key, local_path)
            cached_paths.append(local_path)
        except Exception:
            continue
    return cached_paths


def cache_sku_image(sku, source_path, source_identifier, index):
    s3_client = get_s3_client()
    if not s3_client or not os.path.isfile(source_path):
        return

    digest = sha1(source_identifier.encode("utf-8")).hexdigest()[:12]
    key = f"{image_cache_prefix(sku)}{index:02d}-{digest}.jpg"
    try:
        s3_client.upload_file(source_path, STORAGE_BUCKET, key, ExtraArgs={"ContentType": "image/jpeg"})
    except Exception:
        pass


def search_and_download_sku_images(dbx, sku, temp_dir):
    cached = load_cached_sku_images(sku, temp_dir)
    if cached:
        return cached

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
            optimized_path = prepare_image_for_pdf(local_path)
            downloaded.append(optimized_path)
            cache_sku_image(sku, optimized_path, file_info["path"], len(downloaded))
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

        visible_fields = []
        for gs1_key, display_label in CATALOG_FIELDS:
            value = sku_info.get(gs1_key, "N/A")
            if gs1_key in OPTIONAL_UPLOAD_FIELDS:
                if is_missing_upload_value(value):
                    continue
                if gs1_key == "Customer Discount":
                    value = format_customer_discount_value(value)
                else:
                    value = format_upload_value(value)
                if not value:
                    continue
            else:
                value = value or "N/A"
            visible_fields.append((display_label, value))

        for idx, (display_label, value) in enumerate(visible_fields):
            y_pos = start_y + idx * line_gap

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
        self.cell(0, 10, "Propriety: Bagzone Lifestyles Pvt. Ltd.")

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

    def find_uploaded_column(candidates):
        candidate_keys = {re.sub(r"[^a-z0-9]+", "", candidate.lower()) for candidate in candidates}
        for column in df.columns:
            column_key = re.sub(r"[^a-z0-9]+", "", str(column).lower())
            if column_key in candidate_keys:
                return column
        return None

    sku_col = next(
        (candidate for candidate in ["SKU", "sku", "Article", "article", "ARTICLE", "Item Id", "item_id", "ItemId"] if candidate in df.columns),
        df.columns[0],
    )
    stock_col = next(
        (candidate for candidate in ["Stock", "stock", "STOCK", "Qty", "qty", "QTY", "Quantity"] if candidate in df.columns),
        df.columns[1] if len(df.columns) >= 2 else None,
    )
    rrp_col = next(
        (candidate for candidate in ["RRP", "rrp", "Rrp"] if candidate in df.columns),
        None,
    )
    customer_discount_col = find_uploaded_column(
        ["Customer Discount", "CustomerDiscount", "Customer_Discount", "Customer-Discount", "Cust Discount", "Discount"]
    )
    if stock_col in {rrp_col, customer_discount_col}:
        stock_col = None

    sku_list = df[sku_col].dropna().astype(str).str.strip().tolist()
    sku_list = [sku for sku in sku_list if sku]
    if not sku_list:
        raise ValueError("No SKU values found in the uploaded file.")
    
    def build_optional_lookup(value_col, formatter=format_upload_value):
        lookup = {}
        if value_col is None:
            return lookup
        for _, row in df.iterrows():
            sku_val = str(row[sku_col]).strip().upper()
            if not sku_val or sku_val == "NAN":
                continue
            upload_value = formatter(row.get(value_col, "N/A"))
            if upload_value:
                lookup[sku_val] = upload_value
        return lookup

    stock_lookup = build_optional_lookup(stock_col)
    rrp_lookup = build_optional_lookup(rrp_col)
    customer_discount_lookup = build_optional_lookup(customer_discount_col, format_customer_discount_value)
    
    return df, sku_list, stock_lookup, rrp_lookup, customer_discount_lookup


def pdf_to_bytes(pdf):
    output = pdf.output(dest="S")
    if isinstance(output, str):
        return output.encode("latin-1")
    return bytes(output)


def ensure_job_dirs():
    os.makedirs(JOBS_DIR, exist_ok=True)


def cleanup_old_jobs():
    cutoff = datetime.utcnow() - timedelta(seconds=JOB_TTL_SECONDS)
    stale_jobs = CatalogJob.query.filter(CatalogJob.created_at < cutoff).all()
    for job in stale_jobs:
        delete_job_artifacts(job)
        db.session.delete(job)
    if stale_jobs:
        db.session.commit()
    cleanup_old_image_cache()


def cleanup_user_activity_stack(user_id):
    jobs = (
        CatalogJob.query.filter_by(user_id=user_id)
        .order_by(CatalogJob.created_at.desc())
        .all()
    )
    overflow = jobs[USER_ACTIVITY_LIMIT:]
    terminal_statuses = {"complete", "partial_complete", "failed", "cancelled"}
    stale_jobs = [job for job in overflow if job.status in terminal_statuses]
    for job in stale_jobs:
        delete_job_artifacts(job)
        db.session.delete(job)
    if stale_jobs:
        db.session.commit()


def cleanup_all_user_activity_stacks():
    user_ids = [row[0] for row in db.session.query(User.id).all()]
    for user_id in user_ids:
        cleanup_user_activity_stack(user_id)


def delete_user_with_jobs(user):
    jobs = CatalogJob.query.filter_by(user_id=user.id).all()
    for job in jobs:
        delete_job_artifacts(job)
        db.session.delete(job)
    db.session.delete(user)


def cleanup_old_image_cache():
    s3_client = get_s3_client()
    if not s3_client:
        return

    cutoff = datetime.utcnow().timestamp() - IMAGE_CACHE_SECONDS
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=STORAGE_BUCKET, Prefix=image_cache_root_prefix())
        for page in pages:
            expired_keys = []
            for item in page.get("Contents", []):
                key = item.get("Key", "")
                last_modified = item.get("LastModified")
                if key and last_modified and last_modified.timestamp() < cutoff:
                    expired_keys.append({"Key": key})
            for index in range(0, len(expired_keys), 1000):
                s3_client.delete_objects(Bucket=STORAGE_BUCKET, Delete={"Objects": expired_keys[index : index + 1000]})
    except Exception:
        pass


def get_redis_client():
    if not REDIS_URL:
        return None
    return redis.from_url(REDIS_URL, decode_responses=True)


def get_s3_client():
    if not STORAGE_BUCKET:
        return None
    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT_URL or None,
        region_name=STORAGE_REGION,
        aws_access_key_id=os.getenv("STORAGE_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("STORAGE_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def upload_catalog_file(file_path, file_name):
    s3_client = get_s3_client()
    if not s3_client:
        return ""

    key = f"{STORAGE_PREFIX}/{file_name}" if STORAGE_PREFIX else file_name
    s3_client.upload_file(file_path, STORAGE_BUCKET, key, ExtraArgs={"ContentType": "application/pdf"})
    return key


def delete_storage_key(key):
    if not key:
        return
    s3_client = get_s3_client()
    if not s3_client:
        return
    try:
        s3_client.delete_object(Bucket=STORAGE_BUCKET, Key=key)
    except Exception:
        pass


def delete_job_artifacts(job):
    if not job:
        return
    delete_storage_key(job.storage_key)
    if job.file_path:
        try:
            os.remove(job.file_path)
        except OSError:
            pass


def open_catalog_file(job):
    if job.storage_key:
        s3_client = get_s3_client()
        if not s3_client:
            raise FileNotFoundError("Storage is not configured.")
        response = s3_client.get_object(Bucket=STORAGE_BUCKET, Key=job.storage_key)
        body = response["Body"].read()
        return io.BytesIO(body)

    if job.file_blob:
        return io.BytesIO(job.file_blob)

    if not job.file_path or not os.path.isfile(job.file_path):
        raise FileNotFoundError("Generated catalog file is missing.")
    return job.file_path


def enqueue_job(job_id):
    client = get_redis_client()
    if client:
        client.rpush(QUEUE_NAME, json.dumps({"job_id": job_id}, separators=(",", ":")))
        return
    local_job_queue.put({"job_id": job_id})


def dequeue_job(timeout=5):
    client = get_redis_client()
    if client:
        item = client.blpop(QUEUE_NAME, timeout=timeout)
        if not item:
            return None
        _queue_name, payload = item
        return json.loads(payload)

    try:
        return local_job_queue.get(timeout=timeout)
    except queue.Empty:
        return None


def update_job(job_id, **changes):
    job = db.session.get(CatalogJob, job_id)
    if not job:
        return
    for key, value in changes.items():
        setattr(job, key, value)
    job.updated_at = datetime.utcnow()
    user_id = job.user_id
    db.session.commit()
    if changes.get("status") in {"complete", "partial_complete", "failed", "cancelled"}:
        cleanup_user_activity_stack(user_id)


def public_job(job):
    queued_seconds = 0
    if job.status == "queued" and job.created_at:
        queued_seconds = max(0, int((datetime.utcnow() - job.created_at).total_seconds()))
    message = job.message or ""
    if job.status == "queued" and queued_seconds >= 60:
        message = "Still queued. If this stays here, the worker service or REDIS_URL may not be connected."

    payload = {
        "id": job.id,
        "status": job.status,
        "message": message,
        "current": job.current or 0,
        "total": job.total or 0,
        "percent": job.percent or 0,
        "sku": job.sku or "",
        "fileName": job.file_name or "",
        "summary": job.summary,
        "rows": job.rows or [],
        "error": job.error or "",
        "stopRequested": bool(job.stop_requested),
        "isPartial": bool(job.partial),
        "createdAt": format_datetime(job.created_at),
        "updatedAt": format_datetime(job.updated_at),
        "queuedSeconds": queued_seconds,
    }
    if job.status in {"complete", "partial_complete"}:
        payload["downloadUrl"] = f"/api/jobs/{job.id}/download"
    return payload


def get_job(job_id):
    return db.session.get(CatalogJob, job_id)


def can_access_job(job):
    if not job or not current_user.is_authenticated:
        return False
    return current_user.is_admin or job.user_id == current_user.id


def is_stop_requested(job_id):
    job = get_job(job_id)
    return bool(job and job.stop_requested)


def create_job(user_id, sku_list, stock_lookup, rrp_lookup=None, customer_discount_lookup=None):
    cleanup_old_jobs()
    job_id = uuid.uuid4().hex
    job = CatalogJob(
        id=job_id,
        user_id=user_id,
        status="queued",
        message="Catalog job queued.",
        current=0,
        total=len(sku_list),
        percent=0,
        sku="",
        file_name="",
        file_path="",
        storage_key="",
        error="",
        stop_requested=False,
        partial=False,
        summary=None,
        rows=[],
        payload={
            "sku_list": sku_list,
            "stock_lookup": stock_lookup,
            "rrp_lookup": rrp_lookup or {},
            "customer_discount_lookup": customer_discount_lookup or {},
        },
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.session.add(job)
    db.session.commit()
    cleanup_user_activity_stack(user_id)
    enqueue_job(job.id)
    return job


def requeue_interrupted_jobs():
    interrupted = CatalogJob.query.filter(CatalogJob.status.in_(["running", "stopping"])).all()
    for job in interrupted:
        job.status = "queued"
        job.message = "Catalog job resumed after worker restart."
        job.updated_at = datetime.utcnow()
        enqueue_job(job.id)
    if interrupted:
        db.session.commit()


def mark_stale_queued_jobs_for_worker():
    queued = CatalogJob.query.filter_by(status="queued").all()
    for job in queued:
        enqueue_job(job.id)


def process_job(job_id):
    job = get_job(job_id)
    if not job or job.status not in {"queued", "stopping"}:
        return

    try:
        generate_catalog_file(job_id)
    except ValueError as exc:
        update_job(job_id, status="failed", error=str(exc), message="Catalog generation failed.")
    except FileNotFoundError:
        update_job(job_id, status="failed", error="GS1 master file is missing on the server.", message="Catalog generation failed.")
    except Exception as exc:
        update_job(job_id, status="failed", error=f"Catalog generation failed: {exc}", message="Catalog generation failed.")


def run_worker():
    init_database()
    requeue_interrupted_jobs()
    mark_stale_queued_jobs_for_worker()
    while True:
        payload = dequeue_job(timeout=5)
        if not payload:
            continue
        process_job(payload["job_id"])
        if not REDIS_URL:
            local_job_queue.task_done()


def inline_worker_loop():
    with app.app_context():
        while True:
            payload = dequeue_job(timeout=5)
            if not payload:
                continue
            process_job(payload["job_id"])
            local_job_queue.task_done()


def start_inline_workers_if_needed():
    global inline_workers_started
    if inline_workers_started or REDIS_URL or os.getenv("START_INLINE_WORKERS", "1").lower() not in {"1", "true", "yes"}:
        return
    inline_workers_started = True
    for index in range(MAX_PARALLEL_JOBS):
        worker = threading.Thread(target=inline_worker_loop, name=f"local-catalog-worker-{index + 1}", daemon=True)
        worker.start()


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
    storage_key = upload_catalog_file(file_path, file_name)
    file_blob = None
    if not storage_key:
        with open(file_path, "rb") as pdf_file:
            file_blob = pdf_file.read()

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
        storage_key=storage_key,
        file_blob=file_blob,
        partial=partial,
    )


def generate_catalog_file(job_id, sku_list=None, stock_lookup=None, rrp_lookup=None, customer_discount_lookup=None):
    if sku_list is None or stock_lookup is None:
        job = get_job(job_id)
        if not job:
            raise ValueError("Catalog job was not found.")
        payload = job.payload or {}
        if sku_list is None:
            sku_list = payload["sku_list"]
        if stock_lookup is None:
            stock_lookup = payload.get("stock_lookup", {})
        if rrp_lookup is None:
            rrp_lookup = payload.get("rrp_lookup", {})
        if customer_discount_lookup is None:
            customer_discount_lookup = payload.get("customer_discount_lookup", {})
    elif rrp_lookup is None:
        rrp_lookup = {}
    if customer_discount_lookup is None:
        customer_discount_lookup = {}

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
            info["RRP"] = rrp_lookup.get(sku_upper, "N/A")
            info["Customer Discount"] = customer_discount_lookup.get(sku_upper, "N/A")
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


@app.errorhandler(RequestEntityTooLarge)
def handle_file_too_large(_error):
    return jsonify({"success": False, "error": "Uploaded file is too large."}), 413


@app.get("/login")
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    return render_template("login.html")


@app.post("/login")
def login_post():
    email = normalize_email(request.form.get("email", ""))
    password = request.form.get("password", "")
    if not is_allowed_email(email):
        flash(f"Only @{ALLOWED_EMAIL_DOMAIN} email addresses can sign in.", "error")
        return render_template("login.html"), 403

    user = User.query.filter_by(email=email).first()
    if not user:
        flash("No account found for this email. Please register first.", "error")
        return render_template("login.html", email=email), 401
    if not user.active:
        flash("This account is inactive. Please contact admin.", "error")
        return render_template("login.html", email=email), 403
    if not user.check_password(password):
        flash("Invalid email or password.", "error")
        return render_template("login.html", email=email), 401

    login_user(user)
    next_url = request.args.get("next")
    return redirect(next_url if is_safe_next_url(next_url) else url_for("index"))


@app.get("/register")
def register():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    return render_template("register.html")


@app.post("/register")
def register_post():
    if current_user.is_authenticated:
        return redirect(url_for("index"))

    email = normalize_email(request.form.get("email", ""))
    name = request.form.get("name", "").strip()
    password = request.form.get("password", "")
    confirm_password = request.form.get("confirm_password", "")

    if not email or not password:
        flash("Email and password are required.", "error")
        return render_template("register.html", email=email, name=name), 400
    if not is_allowed_email(email):
        flash(f"Only @{ALLOWED_EMAIL_DOMAIN} email addresses can register.", "error")
        return render_template("register.html", email=email, name=name), 403
    if len(password) < 8:
        flash("Password must be at least 8 characters.", "error")
        return render_template("register.html", email=email, name=name), 400
    if password != confirm_password:
        flash("Passwords do not match.", "error")
        return render_template("register.html", email=email, name=name), 400
    if User.query.filter_by(email=email).first():
        flash("This email is already registered. Please login.", "error")
        return redirect(url_for("login"))

    user = User(id=uuid.uuid4().hex, email=email, name=name or email, role="user", active=True)
    user.set_password(password)
    db.session.add(user)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        flash("This email is already registered. Please login.", "error")
        return redirect(url_for("login"))

    login_user(user)
    flash("Account created. You are logged in.", "success")
    next_url = request.args.get("next")
    return redirect(next_url if is_safe_next_url(next_url) else url_for("index"))


@app.post("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


@app.get("/")
@login_required
def index():
    cleanup_old_jobs()
    cleanup_user_activity_stack(current_user.id)
    recent_jobs = (
        CatalogJob.query.filter_by(user_id=current_user.id)
        .order_by(CatalogJob.created_at.desc())
        .limit(USER_ACTIVITY_LIMIT)
        .all()
    )
    return render_template("index.html", recent_jobs=recent_jobs, activity_limit=USER_ACTIVITY_LIMIT)


@app.get("/assets/<path:filename>")
def public_assets(filename):
    return send_from_directory(os.path.join(PUBLIC_DIR, "assets"), filename)


def build_user_job_stats(users):
    user_ids = [user.id for user in users]
    history_cutoff = datetime.utcnow() - timedelta(days=JOB_RETENTION_DAYS)
    stats = {
        user.id: {
            "total": 0,
            "active": 0,
            "complete": 0,
            "failed": 0,
            "last_job": None,
        }
        for user in users
    }
    if not user_ids:
        return stats

    jobs = (
        CatalogJob.query.filter(CatalogJob.user_id.in_(user_ids), CatalogJob.created_at >= history_cutoff)
        .order_by(CatalogJob.created_at.desc())
        .all()
    )
    for job in jobs:
        item = stats.get(job.user_id)
        if not item:
            continue
        item["total"] += 1
        if job.status in {"queued", "running", "stopping"}:
            item["active"] += 1
        if job.status in {"complete", "partial_complete"}:
            item["complete"] += 1
        if job.status in {"failed", "cancelled"}:
            item["failed"] += 1
        if item["last_job"] is None:
            item["last_job"] = job
    return stats


def can_change_admin_role_or_status(user, role, active):
    if user.id == current_user.id and (role != "admin" or not active):
        return False
    admin_count = User.query.filter_by(role="admin", active=True).count()
    if user.is_admin and user.active and admin_count <= 1 and (role != "admin" or not active):
        return False
    return True


@app.get("/admin/users")
@login_required
def admin_users():
    if not require_admin():
        return jsonify({"success": False, "error": "Admin access is required."}), 403
    cleanup_old_jobs()
    cleanup_all_user_activity_stacks()
    history_cutoff = datetime.utcnow() - timedelta(days=JOB_RETENTION_DAYS)
    users = User.query.order_by(User.created_at.desc()).all()
    recent_jobs = (
        CatalogJob.query.filter(CatalogJob.created_at >= history_cutoff)
        .order_by(CatalogJob.created_at.desc())
        .limit(50)
        .all()
    )
    return render_template(
        "admin_users.html",
        users=users,
        user_stats=build_user_job_stats(users),
        recent_jobs=recent_jobs,
        retention_days=JOB_RETENTION_DAYS,
        activity_limit=USER_ACTIVITY_LIMIT,
    )


@app.post("/admin/users")
@login_required
def create_user():
    if not require_admin():
        return jsonify({"success": False, "error": "Admin access is required."}), 403

    email = normalize_email(request.form.get("email", ""))
    name = request.form.get("name", "").strip()
    password = request.form.get("password", "")
    role = request.form.get("role", "user").strip()
    if role not in {"user", "admin"}:
        role = "user"
    if not email or not password:
        flash("Email and password are required.", "error")
        return redirect(url_for("admin_users"))
    if len(password) < 8:
        flash("Password must be at least 8 characters.", "error")
        return redirect(url_for("admin_users"))
    if not is_allowed_email(email):
        flash(f"Only @{ALLOWED_EMAIL_DOMAIN} email addresses are allowed.", "error")
        return redirect(url_for("admin_users"))
    if User.query.filter_by(email=email).first():
        flash("User already exists.", "error")
        return redirect(url_for("admin_users"))

    user = User(id=uuid.uuid4().hex, email=email, name=name or email, role=role, active=True)
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    flash("User created.", "success")
    return redirect(url_for("admin_users"))


@app.post("/admin/users/<user_id>")
@login_required
def update_user(user_id):
    if not require_admin():
        return jsonify({"success": False, "error": "Admin access is required."}), 403

    user = db.session.get(User, user_id)
    if not user:
        flash("User not found.", "error")
        return redirect(url_for("admin_users"))

    email = normalize_email(request.form.get("email", ""))
    name = request.form.get("name", "").strip()
    password = request.form.get("password", "")
    role = request.form.get("role", "user").strip()
    active = request.form.get("active") == "1"

    if role not in {"user", "admin"}:
        role = "user"
    if not email:
        flash("Email is required.", "error")
        return redirect(url_for("admin_users"))
    if not is_allowed_email(email):
        flash(f"Only @{ALLOWED_EMAIL_DOMAIN} email addresses are allowed.", "error")
        return redirect(url_for("admin_users"))
    existing = User.query.filter(User.email == email, User.id != user.id).first()
    if existing:
        flash("Another user already has this email.", "error")
        return redirect(url_for("admin_users"))
    if not can_change_admin_role_or_status(user, role, active):
        flash("You cannot remove the last active admin or deactivate your own admin account.", "error")
        return redirect(url_for("admin_users"))
    if password and len(password) < 8:
        flash("New password must be at least 8 characters.", "error")
        return redirect(url_for("admin_users"))

    user.email = email
    user.name = name or email
    user.role = role
    user.active = active
    if password:
        user.set_password(password)

    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        flash("Another user already has this email.", "error")
        return redirect(url_for("admin_users"))

    flash("User updated.", "success")
    return redirect(url_for("admin_users"))


@app.post("/admin/cleanup-load-test-users")
@login_required
def cleanup_load_test_users():
    if not require_admin():
        return jsonify({"success": False, "error": "Admin access is required."}), 403

    test_users = (
        User.query.filter(User.email.like("loadtest-%@bagzone.in"))
        .order_by(User.created_at.desc())
        .all()
    )
    deleted_count = 0
    for user in test_users:
        if user.id == current_user.id:
            continue
        delete_user_with_jobs(user)
        deleted_count += 1

    db.session.commit()
    flash(f"Deleted {deleted_count} load test users and their jobs.", "success")
    return redirect(url_for("admin_users"))


@app.get("/admin/users/<user_id>/jobs")
@login_required
def admin_user_jobs(user_id):
    if not require_admin():
        return jsonify({"success": False, "error": "Admin access is required."}), 403

    user = db.session.get(User, user_id)
    if not user:
        flash("User not found.", "error")
        return redirect(url_for("admin_users"))

    cleanup_old_jobs()
    history_cutoff = datetime.utcnow() - timedelta(days=JOB_RETENTION_DAYS)
    cleanup_user_activity_stack(user.id)
    jobs = (
        CatalogJob.query.filter(CatalogJob.user_id == user.id, CatalogJob.created_at >= history_cutoff)
        .order_by(CatalogJob.created_at.desc())
        .limit(USER_ACTIVITY_LIMIT)
        .all()
    )
    return render_template(
        "admin_user_jobs.html",
        user=user,
        jobs=jobs,
        retention_days=JOB_RETENTION_DAYS,
        activity_limit=USER_ACTIVITY_LIMIT,
    )


@app.get("/sample-template")
@login_required
def sample_template():
    sample_df = pd.DataFrame(
        {
            "SKU": pd.Series(dtype="str"),
            "Stock": pd.Series(dtype="int"),
            "RRP": pd.Series(dtype="int"),
            "Customer Discount": pd.Series(dtype="str"),
        }
    )
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
@login_required
def start_catalog_job():
    uploaded_file = request.files.get("sku_file")
    if uploaded_file is None or not uploaded_file.filename:
        return jsonify({"success": False, "error": "Please upload a CSV or XLSX file."}), 400

    try:
        _df, sku_list, stock_lookup, rrp_lookup, customer_discount_lookup = parse_uploaded_file(uploaded_file)
    except ValueError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400

    job = create_job(current_user.id, sku_list, stock_lookup, rrp_lookup, customer_discount_lookup)
    return jsonify({"success": True, "job": public_job(job)}), 202


@app.post("/api/jobs/<job_id>/stop")
@login_required
def stop_catalog_job(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Catalog job was not found or has expired."}), 404
    if not can_access_job(job):
        return jsonify({"success": False, "error": "You do not have access to this catalog job."}), 403
    if job.status in {"complete", "partial_complete", "failed", "cancelled"}:
        return jsonify({"success": True, "job": public_job(job)})
    if job.status not in {"queued", "running", "stopping"}:
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
@login_required
def catalog_job_status(job_id):
    cleanup_old_jobs()
    job = get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Catalog job was not found or has expired."}), 404
    if not can_access_job(job):
        return jsonify({"success": False, "error": "You do not have access to this catalog job."}), 403
    return jsonify({"success": True, "job": public_job(job)})


@app.get("/api/jobs/<job_id>/download")
@login_required
def download_catalog_job(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Catalog job was not found or has expired."}), 404
    if not can_access_job(job):
        return jsonify({"success": False, "error": "You do not have access to this catalog job."}), 403
    if job.status not in {"complete", "partial_complete"}:
        return jsonify({"success": False, "error": "Catalog is not ready yet."}), 409
    try:
        catalog_file = open_catalog_file(job)
    except FileNotFoundError as exc:
        return jsonify({"success": False, "error": str(exc)}), 404

    return send_file(
        catalog_file,
        as_attachment=True,
        download_name=job.file_name or "Catalog.pdf",
        mimetype="application/pdf",
    )


@app.get("/api/jobs")
@login_required
def list_catalog_jobs():
    cleanup_old_jobs()
    if not current_user.is_admin:
        cleanup_user_activity_stack(current_user.id)
    history_cutoff = datetime.utcnow() - timedelta(days=JOB_RETENTION_DAYS)
    query = CatalogJob.query
    if not current_user.is_admin:
        query = query.filter_by(user_id=current_user.id)
    query = query.filter(CatalogJob.created_at >= history_cutoff)
    jobs = query.order_by(CatalogJob.created_at.desc()).limit(USER_ACTIVITY_LIMIT if not current_user.is_admin else 25).all()
    return jsonify({"success": True, "jobs": [public_job(job) for job in jobs]})


@app.post("/api/generate")
@login_required
def generate_catalog():
    return jsonify({"success": False, "error": "Use /api/jobs. Direct web-process generation is disabled."}), 410


@app.post("/api/generate-stream")
@login_required
def generate_catalog_stream():
    return jsonify({"success": False, "error": "Use /api/jobs. Streaming web-process generation is disabled."}), 410


def legacy_generate_catalog_disabled():
    return None


def legacy_generate_catalog_stream_disabled():
    return None


with app.app_context():
    init_database()
    start_inline_workers_if_needed()


if __name__ == "__main__":
    app.run(debug=True)
