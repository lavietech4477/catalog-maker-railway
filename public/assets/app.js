const form = document.querySelector("#catalog-form");
const fileInput = document.querySelector("#sku-file");
const fileName = document.querySelector("#file-name");
const generateBtn = document.querySelector("#generate-btn");
const statusCard = document.querySelector("#status-card");
const statusTitle = document.querySelector("#status-title");
const statusText = document.querySelector("#status-text");
const progressBar = document.querySelector("#progress-bar");
const progressCount = document.querySelector("#progress-count");
const progressPercent = document.querySelector("#progress-percent");
const resultCard = document.querySelector("#result-card");
const downloadBtn = document.querySelector("#download-btn");
const downloadLogBtn = document.querySelector("#download-log-btn");
const stopBtn = document.querySelector("#stop-btn");
const resultTitle = document.querySelector("#result-title");
const reportBody = document.querySelector("#report-body");

let currentDownloadUrl = "";
let currentFileName = "Catalog.pdf";
let currentReportRows = [];
let currentJobId = "";
let pollTimer = null;
let stopRequested = false;

fileInput.addEventListener("change", () => {
  const file = fileInput.files[0];
  fileName.textContent = file ? file.name : "CSV or XLSX";
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const file = fileInput.files[0];
  if (!file) {
    showStatus("File missing", "Please select a CSV or XLSX file.", true);
    return;
  }

  clearPollTimer();
  const formData = new FormData();
  formData.append("sku_file", file);

  generateBtn.disabled = true;
  stopBtn.disabled = false;
  stopBtn.classList.add("hidden");
  resultCard.classList.add("hidden");
  currentDownloadUrl = "";
  currentFileName = "Catalog.pdf";
  currentReportRows = [];
  currentJobId = "";
  stopRequested = false;
  reportBody.innerHTML = "";
  setProgress(0, 0, 0);
  showStatus("Starting catalog job", "Uploading SKU file and preparing background job.", false);

  try {
    const response = await fetch("/api/jobs", {
      method: "POST",
      body: formData,
    });
    const payload = await response.json();

    if (!response.ok || !payload.success) {
      throw new Error(payload.error || "Catalog generation failed.");
    }

    currentJobId = payload.job.id;
    handleJob(payload.job);
    pollJob(currentJobId);
  } catch (error) {
    showStatus("Catalog not generated", error.message, true);
    generateBtn.disabled = false;
    stopBtn.classList.add("hidden");
  }
});

stopBtn.addEventListener("click", async () => {
  if (!currentJobId || stopRequested) return;

  stopRequested = true;
  stopBtn.disabled = true;
  statusTitle.textContent = "Stopping catalog";
  statusText.textContent = "Finishing the current SKU and preparing a partial PDF.";

  try {
    const response = await fetch(`/api/jobs/${currentJobId}/stop`, {
      method: "POST",
    });
    const payload = await response.json();

    if (!response.ok || !payload.success) {
      throw new Error(payload.error || "Unable to stop catalog generation.");
    }

    handleJob(payload.job);
  } catch (error) {
    stopRequested = false;
    stopBtn.disabled = false;
    showStatus("Stop request failed", error.message, true);
  }
});

downloadBtn.addEventListener("click", () => {
  if (!currentDownloadUrl) return;
  const link = document.createElement("a");
  link.href = currentDownloadUrl;
  link.download = currentFileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
});

if (downloadLogBtn) {
  downloadLogBtn.addEventListener("click", () => {
    downloadLogCsv(currentReportRows);
  });
}


async function pollJob(jobId) {
  clearPollTimer();
  pollTimer = window.setInterval(async () => {
    try {
      const response = await fetch(`/api/jobs/${jobId}`, { cache: "no-store" });
      const payload = await response.json();

      if (!response.ok || !payload.success) {
        throw new Error(payload.error || "Unable to read catalog job status.");
      }

      handleJob(payload.job);
    } catch (error) {
      clearPollTimer();
      showStatus("Catalog status unavailable", error.message, true);
      generateBtn.disabled = false;
    }
  }, 2500);
}

function handleJob(job) {
  if (job.status === "queued") {
    statusTitle.textContent = "Catalog queued";
    statusText.textContent = job.message || "Waiting for the next worker.";
    setProgress(job.percent || 0, job.current || 0, job.total || 0);
    showStopButton();
    return;
  }

  if (job.status === "running") {
    statusTitle.textContent = "Generating catalog";
    statusText.textContent = job.message || `Processing ${job.sku}`;
    setProgress(job.percent || 0, job.current || 0, job.total || 0);
    showStopButton();
    return;
  }

  if (job.status === "stopping") {
    statusTitle.textContent = "Stopping catalog";
    statusText.textContent = job.message || "Creating partial PDF.";
    setProgress(job.percent || 0, job.current || 0, job.total || 0);
    stopRequested = true;
    stopBtn.disabled = true;
    stopBtn.classList.remove("hidden");
    return;
  }

  if (job.status === "failed") {
    clearPollTimer();
    showStatus("Catalog not generated", job.error || "Catalog generation failed.", true);
    generateBtn.disabled = false;
    stopBtn.classList.add("hidden");
    return;
  }

  if (job.status === "cancelled") {
    clearPollTimer();
    showStatus("Catalog stopped", job.message || "No catalog pages were created.", true);
    generateBtn.disabled = false;
    stopBtn.classList.add("hidden");
    return;
  }

  if (job.status === "complete" || job.status === "partial_complete") {
    clearPollTimer();
    statusCard.classList.add("hidden");
    generateBtn.disabled = false;
    stopBtn.classList.add("hidden");
    renderResult(job);
  }
}

function showStatus(title, text, isError) {
  statusTitle.textContent = title;
  statusText.textContent = text;
  statusCard.classList.remove("hidden");
  statusCard.style.background = isError ? "#ffffff" : "";
}

function clearPollTimer() {
  if (pollTimer) {
    window.clearInterval(pollTimer);
    pollTimer = null;
  }
}

function showStopButton() {
  stopBtn.classList.remove("hidden");
  stopBtn.disabled = stopRequested;
}

function setProgress(percent, current, total) {
  const safePercent = Math.max(0, Math.min(100, Number(percent) || 0));
  const safeCurrent = Number(current) || 0;
  const safeTotal = Number(total) || 0;
  progressBar.style.width = `${safePercent}%`;
  progressCount.textContent = `${safeCurrent} / ${safeTotal} SKUs`;
  progressCount.dataset.current = String(safeCurrent);
  progressCount.dataset.total = String(safeTotal);
  progressPercent.textContent = `${safePercent}%`;
}

function renderResult(job) {
  currentDownloadUrl = job.downloadUrl || "";
  currentFileName = job.fileName || "Catalog.pdf";
  currentReportRows = Array.isArray(job.rows) ? job.rows : [];
  resultTitle.textContent = job.isPartial ? "Partial catalog ready" : "Catalog ready";
  downloadBtn.textContent = job.isPartial ? "Download Partial PDF" : "Download PDF";

  document.querySelector("#pages-count").textContent = job.summary.pages;
  document.querySelector("#data-count").textContent = job.summary.withProductData;
  document.querySelector("#images-count").textContent = job.summary.imagesFetched;
  document.querySelector("#missing-count").textContent = job.summary.missingImages;

  reportBody.innerHTML = "";
  currentReportRows.forEach((row) => {
    const tr = document.createElement("tr");
    if (isFailedLogRow(row)) {
      tr.classList.add("report-row-failed");
    }
    tr.innerHTML = `
      <td>${escapeHtml(row.sku)}</td>
      <td>${escapeHtml(row.brand)}</td>
      <td>${escapeHtml(row.title)}</td>
      <td>${escapeHtml(row.newMrp)}</td>
      <td>${escapeHtml(String(row.images))}</td>
      <td>${escapeHtml(row.gs1)}</td>
      <td>${escapeHtml(row.dropbox)}</td>
    `;
    reportBody.appendChild(tr);
  });

  resultCard.classList.remove("hidden");
}

function isFailedLogRow(row) {
  const imageCount = Number(row.images) || 0;
  const gs1Status = String(row.gs1 || "").trim().toLowerCase();
  const dropboxStatus = String(row.dropbox || "").trim().toLowerCase();
  return imageCount <= 0 || gs1Status !== "found" || dropboxStatus.includes("no image");
}

function downloadLogCsv(rows) {
  if (!rows.length) return;

  const headers = ["SKU", "Brand", "Title", "NEW MRP", "Images", "GS1", "Dropbox"];
  const csvRows = [
    headers.map(csvCell).join(","),
    ...rows.map((row) =>
      [
        row.sku,
        row.brand,
        row.title,
        row.newMrp,
        row.images,
        row.gs1,
        row.dropbox,
      ]
        .map(csvCell)
        .join(",")
    ),
  ];

  const blob = new Blob([`\ufeff${csvRows.join("\r\n")}`], {
    type: "text/csv;charset=utf-8;",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = buildLogFileName();
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function buildLogFileName() {
  return currentFileName.replace(/\.pdf$/i, "_log.csv") || "Catalog_log.csv";
}

function csvCell(value) {
  const text = String(value ?? "");
  return `"${text.replaceAll('"', '""')}"`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
