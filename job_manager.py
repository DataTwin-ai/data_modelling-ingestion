"""
Background ingestion job manager for the dashboard.

- Keeps an in-memory registry of upload-triggered ingestion jobs.
- Runs Job_Handler in a thread pool so uploads return immediately.
- Captures every line printed while a job runs (clog + plain print, stdout +
  stderr) into that job's own log, using a ContextVar so concurrent jobs never
  mix their logs.
- Tracks the current pipeline stage of each job (set_stage) for live progress.
- Persists each job's log (.log) and summary (.json) under logs/jobs/ so history
  survives a server restart.

NOTE: the registry lives in process memory, so run the API with a single
uvicorn worker (the default in api.py).

This module must not import app.py (app.py imports it for the log hook).
"""
import os
import re
import json
import threading
import contextvars
import traceback
import configparser
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOBS_DIR = os.path.join(BASE_DIR, "logs", "jobs")
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
# Input files the dashboard may hand back for download (never arbitrary paths)
INPUT_FILE_EXTENSIONS = {".xlsx", ".xls", ".xlsm", ".csv"}

_config = configparser.ConfigParser(interpolation=None)
_config.read(os.path.join(BASE_DIR, "config.ini"))
MAX_CONCURRENT_JOBS = max(1, _config.getint("defaults", "max_concurrent_jobs", fallback=2))
HISTORY_LIMIT = 200          # jobs reloaded from disk on startup
MAX_EVENTS_IN_MEMORY = 5000  # per job; full log is always kept on disk

# Pipeline stages in order (FAILED / INTERRUPTED are terminal alternatives).
STAGES = [
    "QUEUED",
    "READING_FILE",
    "AI_CLASSIFICATION",
    "PROCESS_METADATA",
    "THEME_MAPPING",
    "UPSERT",
    "EXPORT",
    "COMPLETED",
]
TERMINAL_STATUSES = {"completed", "failed", "interrupted"}
ACTIVE_STATUSES = {"queued", "running"}

# Fraction of a single sheet's work done when each per-sheet stage starts.
_SHEET_STAGE_FRACTION = {
    "AI_CLASSIFICATION": 0.0,
    "PROCESS_METADATA": 0.3,
    "THEME_MAPPING": 0.5,
    "UPSERT": 0.7,
}

_current_job = contextvars.ContextVar("current_ingestion_job_id", default=None)
_lock = threading.RLock()
_jobs = {}            # job_id -> Job
_global_events = []   # [(seq, summary_dict)] job-summary changes for the dashboard list
_global_seq = 0
_GLOBAL_EVENTS_CAP = 2000

_executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS, thread_name_prefix="ingest-job")

_TS_PREFIX_RE = re.compile(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\]\s*")
_LEVEL_PREFIX_RE = re.compile(r"^\S{0,4}\s*\[(INFO|SUCCESS|WARNING|ERROR|DEBUG)\]\s*")


def _now():
    return datetime.now().isoformat(timespec="seconds")


class JobConflictError(Exception):
    """Raised when an active job is already ingesting the same file."""


class Job:
    def __init__(self, job_id, tenant_id, org_id, file_name, file_path, file_size):
        self.job_id = job_id
        self.transaction_id = job_id
        self.tenant_id = tenant_id
        self.org_id = org_id
        self.file_name = file_name
        self.file_path = file_path
        self.file_size = file_size
        self.status = "queued"
        self.stage = "QUEUED"
        self.stage_detail = None
        self.progress = 0
        self.sheet_index = None
        self.total_sheets = None
        self.created_at = _now()
        self.started_at = None
        self.finished_at = None
        self.error = None
        self.result = None

        self.events = []        # [{id, type, ...}]
        self._first_event_id = 1
        self._seq = 0
        self._buffers = {"out": "", "err": ""}
        self._log_fh = None

    # ---------------- persistence ----------------
    @property
    def log_path(self):
        return os.path.join(JOBS_DIR, f"{self.job_id}.log")

    @property
    def meta_path(self):
        return os.path.join(JOBS_DIR, f"{self.job_id}.json")

    def input_file(self):
        """Path of the workbook this job ingested, if it is still on disk and is a spreadsheet.
        Table jobs (table://...) and S3 keys have no local input file."""
        p = str(self.file_path or "")
        if not p or "://" in p or os.path.splitext(p)[1].lower() not in INPUT_FILE_EXTENSIONS:
            return None
        return p if os.path.isfile(p) else None

    def summary(self):
        return {
            "input_available": self.input_file() is not None,
            "job_id": self.job_id,
            "transaction_id": self.transaction_id,
            "tenant_id": self.tenant_id,
            "org_id": self.org_id,
            "file_name": self.file_name,
            "file_size": self.file_size,
            "status": self.status,
            "stage": self.stage,
            "stage_detail": self.stage_detail,
            "progress": self.progress,
            "sheet_index": self.sheet_index,
            "total_sheets": self.total_sheets,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error": self.error,
            "result": self.result,
        }

    def _persist_meta(self):
        try:
            os.makedirs(JOBS_DIR, exist_ok=True)
            data = self.summary()
            data["file_path"] = self.file_path
            tmp = self.meta_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            os.replace(tmp, self.meta_path)
        except Exception:
            pass

    def _write_log_file(self, ts, level, msg):
        try:
            if self._log_fh is None or self._log_fh.closed:
                os.makedirs(JOBS_DIR, exist_ok=True)
                self._log_fh = open(self.log_path, "a", encoding="utf-8")
            self._log_fh.write(f"{ts}\t{level}\t{msg}\n")
            self._log_fh.flush()
        except Exception:
            pass

    def close_log(self):
        try:
            if self._log_fh and not self._log_fh.closed:
                self._log_fh.close()
        except Exception:
            pass

    # ---------------- events ----------------
    def _add_event(self, event):
        self._seq += 1
        event["id"] = self._seq
        self.events.append(event)
        if len(self.events) > MAX_EVENTS_IN_MEMORY:
            drop = len(self.events) - MAX_EVENTS_IN_MEMORY
            del self.events[:drop]
            self._first_event_id = self.events[0]["id"]

    def add_log(self, msg, level="INFO", ts=None, persist=True):
        ts = ts or _now()
        self._add_event({"type": "log", "ts": ts, "level": level, "msg": msg})
        if persist:
            self._write_log_file(ts, level, msg)

    def events_since(self, last_id):
        start = max(0, (last_id + 1) - self._first_event_id)
        return self.events[start:]

    def feed_text(self, text, is_err=False):
        key = "err" if is_err else "out"
        buf = self._buffers[key] + text
        lines = buf.split("\n")
        self._buffers[key] = lines.pop()
        for line in lines:
            self._emit_line(line, is_err)

    def flush_buffers(self):
        for key in ("out", "err"):
            rest = self._buffers[key]
            self._buffers[key] = ""
            if rest.strip():
                self._emit_line(rest, key == "err")

    def _emit_line(self, line, is_err):
        line = line.rstrip("\r")
        if not line.strip():
            return
        msg = _TS_PREFIX_RE.sub("", line)
        level = None
        m = _LEVEL_PREFIX_RE.match(msg)
        if m:
            level = m.group(1)
            msg = msg[m.end():]
        if level is None:
            if is_err:
                low = msg.lower()
                level = "ERROR" if ("error" in low or "traceback" in low or "exception" in low) else "WARNING"
            else:
                level = "INFO"
        self.add_log(msg, level)


# -------------------------------------------------------------------------
# Global (dashboard list) change feed
# -------------------------------------------------------------------------
def _publish(job):
    """Record a job-summary change; caller must hold _lock."""
    global _global_seq
    _global_seq += 1
    _global_events.append((_global_seq, job.summary()))
    if len(_global_events) > _GLOBAL_EVENTS_CAP:
        del _global_events[: len(_global_events) - _GLOBAL_EVENTS_CAP]
    job._persist_meta()


def global_events_since(last_seq):
    with _lock:
        return [(s, d) for (s, d) in _global_events if s > last_seq], _global_seq


def current_global_seq():
    with _lock:
        return _global_seq


# -------------------------------------------------------------------------
# Hooks used by app.py
# -------------------------------------------------------------------------
def capture_output(text, is_err=False):
    """Called from DualLogger.write for every chunk written to stdout/stderr."""
    job_id = _current_job.get()
    if not job_id:
        return
    with _lock:
        job = _jobs.get(job_id)
        if job is not None:
            job.feed_text(text, is_err)


def set_stage(stage, sheet_index=None, total_sheets=None, detail=None):
    """Mark the current job's pipeline stage. No-op outside a dashboard job."""
    job_id = _current_job.get()
    if not job_id:
        return
    with _lock:
        job = _jobs.get(job_id)
        if job is None:
            return
        job.flush_buffers()
        job.stage = stage
        job.stage_detail = detail
        if total_sheets is not None:
            job.total_sheets = total_sheets
        if sheet_index is not None:
            job.sheet_index = sheet_index

        if stage == "READING_FILE":
            progress = 5
        elif stage in _SHEET_STAGE_FRACTION and job.total_sheets:
            idx = (job.sheet_index or 1) - 1
            progress = 10 + int(80 * (idx + _SHEET_STAGE_FRACTION[stage]) / job.total_sheets)
        elif stage == "EXPORT":
            progress = 92
        elif stage == "COMPLETED":
            progress = 100
        else:
            progress = job.progress
        job.progress = max(job.progress, progress)

        sheet_txt = ""
        if job.sheet_index and job.total_sheets and stage in _SHEET_STAGE_FRACTION:
            sheet_txt = f" [sheet {job.sheet_index}/{job.total_sheets}]"
        ts = _now()
        job._add_event({
            "type": "stage", "ts": ts, "stage": stage, "progress": job.progress,
            "sheet_index": job.sheet_index, "total_sheets": job.total_sheets, "detail": detail,
        })
        job.add_log(f"Stage -> {stage}{sheet_txt}{' : ' + str(detail) if detail else ''}", "STAGE", ts=ts)
        _publish(job)


# -------------------------------------------------------------------------
# Job lifecycle
# -------------------------------------------------------------------------
def _source_key(path):
    """Identity of a job's source: table:// URIs as-is, file paths normalised."""
    path = str(path or "")
    if "://" in path:
        return path.lower()
    return os.path.normcase(os.path.abspath(path))


def create_job(job_id, tenant_id, org_id, file_name, file_path, file_size, kind="upload"):
    """
    Register a queued job. Raises JobConflictError if the same source is already in flight
    (or the job_id is already active).
    kind: 'upload' (dashboard upload), 'file' (POST /api/ingest/file) or 'table' (POST /api/ingest/table).
    """
    norm = _source_key(file_path)
    with _lock:
        existing = _jobs.get(job_id)
        if existing is not None and existing.status in ACTIVE_STATUSES:
            raise JobConflictError(f"Job '{job_id}' is already running.")
        for other in _jobs.values():
            if other.status in ACTIVE_STATUSES and _source_key(other.file_path) == norm:
                raise JobConflictError(
                    f"'{file_name}' is already being ingested for this tenant/org (job {other.job_id})."
                )
        job = Job(job_id, tenant_id, org_id, file_name, file_path, file_size)
        _jobs[job_id] = job
        if kind == "table":
            job.add_log(f"Table ingestion requested: '{file_path}' for tenant={tenant_id}, org={org_id}", "INFO")
        elif kind == "file":
            job.add_log(f"File ingestion requested: '{file_path}' for tenant={tenant_id}, org={org_id}", "INFO")
        else:
            job.add_log(f"Upload received: '{file_name}' ({file_size} bytes) for tenant={tenant_id}, org={org_id}", "INFO")
        job.add_log("Stage -> QUEUED", "STAGE")
        _publish(job)
        return job


def submit(job_id, fn):
    """Run fn() (a zero-arg callable returning the Job_Handler result) in the pool."""
    _executor.submit(_run_job, job_id, fn)


def run_inline(job_id, fn):
    """
    Run fn() as a tracked job in the calling thread (for synchronous API endpoints):
    the dashboard shows its stages and log live, and the call returns the final summary.
    """
    _run_job(job_id, fn)
    return job_summary(job_id)


def _run_job(job_id, fn):
    token = _current_job.set(job_id)
    try:
        with _lock:
            job = _jobs[job_id]
            job.status = "running"
            job.started_at = _now()
            job.add_log("Ingestion started", "INFO")
            _publish(job)

        result = fn()

        safe_result = json.loads(json.dumps(result, default=str)) if result is not None else None
        status_txt = str(safe_result.get("status")) if isinstance(safe_result, dict) else ""
        with _lock:
            job.flush_buffers()
            job.result = safe_result
            if status_txt.lower() == "success":
                job.status = "completed"
            else:
                job.status = "failed"
                job.error = f"Ingestion finished with status: {status_txt or 'unknown'}"
        if job.status == "completed":
            set_stage("COMPLETED")
            with _lock:
                job.add_log("Ingestion completed successfully", "SUCCESS")
        else:
            with _lock:
                job.add_log(job.error, "ERROR")
    except Exception as e:
        tb_lines = traceback.format_exc().rstrip().splitlines()
        try:
            import sys
            sys.__stderr__.write("\n".join(tb_lines) + "\n")  # console only
        except Exception:
            pass
        with _lock:
            job = _jobs.get(job_id)
            if job is not None:
                job.flush_buffers()
                for line in tb_lines:
                    job.add_log(line, "ERROR")
                job.status = "failed"
                job.error = str(e) or e.__class__.__name__
                job.add_log(f"Ingestion failed: {job.error}", "ERROR")
    finally:
        with _lock:
            job = _jobs.get(job_id)
            if job is not None:
                job.flush_buffers()
                job.finished_at = _now()
                if job.status == "failed":
                    job.stage_detail = f"Failed during {job.stage}"
                job._add_event({"type": "done", "ts": job.finished_at, "status": job.status})
                _publish(job)
                job.close_log()
        _current_job.reset(token)


# -------------------------------------------------------------------------
# Queries
# -------------------------------------------------------------------------
def get_job(job_id):
    with _lock:
        return _jobs.get(job_id)


def job_summary(job_id):
    with _lock:
        job = _jobs.get(job_id)
        return job.summary() if job else None


def list_jobs(limit=100):
    with _lock:
        jobs = sorted(_jobs.values(), key=lambda j: j.created_at, reverse=True)
        return [j.summary() for j in jobs[:limit]]


def job_events_since(job_id, last_id):
    """Return (events, is_finished) for SSE streaming."""
    with _lock:
        job = _jobs.get(job_id)
        if job is None:
            return None, True
        return list(job.events_since(last_id)), job.status in TERMINAL_STATUSES


# -------------------------------------------------------------------------
# History reload on startup
# -------------------------------------------------------------------------
def _load_history():
    if not os.path.isdir(JOBS_DIR):
        return
    metas = [os.path.join(JOBS_DIR, f) for f in os.listdir(JOBS_DIR) if f.endswith(".json")]
    metas.sort(key=os.path.getmtime, reverse=True)
    for path in metas[:HISTORY_LIMIT]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                d = json.load(f)
            job = Job(d["job_id"], d.get("tenant_id"), d.get("org_id"), d.get("file_name"),
                      d.get("file_path", ""), d.get("file_size", 0))
            for k in ("status", "stage", "stage_detail", "progress", "sheet_index", "total_sheets",
                      "created_at", "started_at", "finished_at", "error", "result"):
                if k in d:
                    setattr(job, k, d[k])
            interrupted = job.status in ACTIVE_STATUSES
            if interrupted:
                job.status = "interrupted"
                job.error = "Server restarted while this job was running."
            if os.path.exists(job.log_path):
                with open(job.log_path, "r", encoding="utf-8", errors="replace") as lf:
                    lines = lf.readlines()[-MAX_EVENTS_IN_MEMORY:]
                for raw in lines:
                    parts = raw.rstrip("\n").split("\t", 2)
                    if len(parts) == 3:
                        job.add_log(parts[2], parts[1], ts=parts[0], persist=False)
            if interrupted:
                job.add_log(job.error, "ERROR")
                job._persist_meta()
            job._add_event({"type": "done", "ts": job.finished_at, "status": job.status})
            _jobs[job.job_id] = job
        except Exception:
            continue


_load_history()
