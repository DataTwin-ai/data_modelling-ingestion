import os
import re
import sys
import json
import uuid
import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query, status, Request, File, Form, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from pydantic import BaseModel, Field
import psycopg2
import configparser

# Read configuration from config.ini
config_file_path = os.path.join(os.path.dirname(__file__), 'config.ini')
config = configparser.ConfigParser(interpolation=None)
config.read(config_file_path)

env = config.get('environment', 'current_env', fallback='development')
ec2IP = config.get(env, 'ec2_ip')
port = config.getint(env, 'port')
db_user = config.get(env, 'db_user')
db_password = config.get(env, 'db_password')

# Import core ingestion handler and database helpers from app.py
from app import Job_Handler, read_table_data, ec2confic, clog
import job_manager

app = FastAPI(
    title="Data Modelling & Ingestion API",
    description="Unified API Engine supporting automated data ingestion from both Local/S3 Files (.xlsx, .csv) and Database Tables into PostgreSQL active tables.",
    version="2.0.0"
)

# Enable CORS for frontend/client integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================================
# Request & Response Schemas
# =========================================================================

class TargetOverride(BaseModel):
    """Optional: skip AI classification when the caller already knows the target process."""
    process_code: Optional[str] = Field(None, description="Target process code, e.g. 'BT3I' (skips AI classification; orgid is forced to org_id)", example="BT3I")
    transaction_type: Optional[str] = Field(None, description="Transaction type to use with process_code, e.g. 'Invoice'", example="Invoice")
    theme_name: Optional[str] = Field(None, description="Existing theme in theme_mapping to use (default: automatic)", example=None)
    wait: bool = Field(True, description="true = respond when ingestion finished; false = queue it and return job_id immediately")

class TableIngestRequest(TargetOverride):
    db: str = Field(..., description="Source database name (e.g. 'dm00005')", example="dm00005")
    table_name: str = Field(..., description="Source table name to fetch and ingest", example="bsid_view")
    tenant_id: Optional[str] = Field(None, description="Target Tenant ID (defaults to config: DM00005)", example="DM00005")
    org_id: Optional[str] = Field(None, description="Organization ID (defaults to config: OM0000000002)", example="OM0000000002")
    product_name: Optional[str] = Field(None, description="Product Name (defaults to 'DTFOWS')", example="DTFOWS")
    user_id: Optional[str] = Field(None, description="User email/ID (defaults to config)", example="dilip@datatwin.ai")
    transaction_id: Optional[str] = Field(None, description="Custom Transaction ID (auto-generated if omitted)", example="TR_TABLE_INGEST_001")
    save_location: Optional[str] = Field(None, description="Output export folder (defaults to 'exports')", example="exports")

class FileIngestRequest(TargetOverride):
    file_path: str = Field(..., description="Absolute local path or S3 key to the workbook (.xlsx, .csv)", example="C:\\VR3 PROJECT\\injection\\Audited_Books_GSTR-9_CUSAT_FY2022-23 copy.xlsx")
    tenant_id: Optional[str] = Field(None, description="Target Tenant ID (defaults to config: DM00005)", example="DM00005")
    org_id: Optional[str] = Field(None, description="Organization ID (defaults to config: OM0000000002)", example="OM0000000002")
    product_name: Optional[str] = Field(None, description="Product Name (defaults to 'DTFOWS')", example="DTFOWS")
    user_id: Optional[str] = Field(None, description="User email/ID (defaults to config)", example="dilip@datatwin.ai")
    transaction_id: Optional[str] = Field(None, description="Custom Transaction ID (auto-generated if omitted)", example="TR_FILE_INGEST_001")
    save_location: Optional[str] = Field(None, description="Output export folder (defaults to 'exports')", example="exports")

class UnifiedIngestRequest(TargetOverride):
    source_type: str = Field(..., description="Source type: 'table' or 'file'", example="table")
    db: Optional[str] = Field(None, description="Source database name (required if source_type='table')", example="dm00005")
    table_name: Optional[str] = Field(None, description="Source table name (required if source_type='table')", example="bsid_view")
    file_path: Optional[str] = Field(None, description="File path or S3 key (required if source_type='file')", example="C:\\VR3 PROJECT\\injection\\BSID_VIEW.xlsx")
    tenant_id: Optional[str] = Field(None, description="Target Tenant ID", example="DM00005")
    org_id: Optional[str] = Field(None, description="Organization ID", example="OM0000000002")
    product_name: Optional[str] = Field(None, description="Product Name", example="DTFOWS")
    user_id: Optional[str] = Field(None, description="User email/ID", example="dilip@datatwin.ai")
    transaction_id: Optional[str] = Field(None, description="Transaction ID", example="TR_UNIFIED_001")
    save_location: Optional[str] = Field(None, description="Output export folder", example="exports")

class TableInfo(BaseModel):
    table_name: str
    row_count: int

# =========================================================================
# API Endpoints
# =========================================================================

@app.get("/", tags=["Health"])
@app.get("/api/health", tags=["Health"])
def health_check():
    """
    Health check endpoint returning system status and database environment.
    """
    return {
        "status": "online",
        "service": "Data Modelling & Ingestion API",
        "version": "2.0.0",
        "environment": env,
        "database_host": ec2IP,
        "database_port": port,
        "supported_sources": ["table", "file"],
        "docs_url": "/docs"
    }

@app.get("/api/tables", response_model=List[TableInfo], tags=["Discovery"])
def list_database_tables(db: str = Query("dm00005", description="Database name to inspect")):
    """
    Lists all tables in the configured schema (table_schema) of the specified database along with their row counts.
    """
    try:
        conn = ec2confic(db)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.table_name, COALESCE(s.n_live_tup, 0) as row_count
                FROM information_schema.tables t
                LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name AND s.schemaname = t.table_schema
                WHERE t.table_schema = current_schema()
                ORDER BY t.table_name;
            """)
            tables_with_counts = [TableInfo(table_name=r[0], row_count=int(r[1])) for r in cur.fetchall()]
        conn.close()
        return tables_with_counts
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch tables from database '{db}': {str(e)}"
        )

def _tracked_ingest(kind: str, label: str, source_uri: str, req, handler_kwargs: Dict[str, Any], what: str):
    """
    Runs Job_Handler as a dashboard job (stages + live log in /dashboard and /api/jobs).
    wait=True: same synchronous contract as before; wait=False: queued, returns job_id.
    """
    tenant = (req.tenant_id or config.get('defaults', 'tenant_id', fallback='') or "").strip()
    org = (req.org_id or config.get('defaults', 'org_id', fallback='') or "").strip()
    job_id = req.transaction_id if (req.transaction_id and _ID_RE.match(req.transaction_id)) \
        else _new_transaction_id("TBL" if kind == "table" else "FILE")
    try:
        job_manager.create_job(job_id, tenant, org, label, source_uri, 0, kind=kind)
    except job_manager.JobConflictError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

    def run():
        return Job_Handler(
            tenant_id=req.tenant_id,
            org_id=req.org_id,
            product_name=req.product_name,
            user_id=req.user_id,
            transaction_id=req.transaction_id,   # None keeps the config default, as before
            save_location=req.save_location,
            process_code=req.process_code,
            transaction_type=req.transaction_type,
            theme_name=req.theme_name,
            **handler_kwargs,
        )

    clog(f"API Request: Ingest from {kind} '{label}' (job {job_id}, process_code={req.process_code or 'AI'})", "INFO")
    if not req.wait:
        job_manager.submit(job_id, run)
        return {
            "success": True,
            "message": f"{what} accepted. Ingestion queued.",
            "job_id": job_id,
            "transaction_id": job_id,
            "status": "queued",
            "job_url": f"/api/jobs/{job_id}",
            "stream_url": f"/api/jobs/{job_id}/stream",
        }

    summary = job_manager.run_inline(job_id, run) or {}
    result = summary.get("result")
    if result is None:
        clog(f"API Error in {kind} ingestion: {summary.get('error')}", "ERROR")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"{kind.capitalize()} ingestion failed: {summary.get('error')} (job {job_id})"
        )
    ok = summary.get("status") == "completed"
    return {
        "success": ok,
        "message": f"Successfully ingested {what}" if ok else f"Ingestion of {what} finished with status: {result.get('status')}",
        "job_id": job_id,
        "status": summary.get("status"),
        "data": result
    }


@app.post("/api/ingest/table", tags=["Ingestion"])
def ingest_from_table(req: TableIngestRequest):
    """
    **Ingest data directly from a Database Table**:
    1. Fetches all records and columns from the specified `db` and `table_name`.
    2. Uses AI to identify transaction type, process code, and theme
       (skipped when `process_code` is given, e.g. `BT3I` + `transaction_type: Invoice`).
    3. Builds process and theme mapping for ALL columns in the table.
    4. Computes deterministic row checksums.
    5. Performs idempotent conditional UPSERT into `active_{process_code}`.
    6. Automatically exports reverse-mapped Excel (.xlsx) and CSV to `save_location`.

    Every call is a dashboard job (live stages + log at `/dashboard`, `/api/jobs/{job_id}`).
    """
    return _tracked_ingest(
        "table", f"{req.db}.{req.table_name}", f"table://{req.db}/{req.table_name}", req,
        {"source_type": "table", "source_db": req.db, "source_table": req.table_name},
        f"table '{req.table_name}' from database '{req.db}'",
    )

@app.post("/api/ingest/file", tags=["Ingestion"])
def ingest_from_file(req: FileIngestRequest):
    """
    **Ingest data from an Excel/CSV File**:
    1. Reads all sheets from local file path or S3 key.
    2. Runs AI classification (or uses `process_code`), process cataloging, and theme mapping for each sheet.
    3. Performs idempotent conditional UPSERT into the target active table.
    4. Automatically exports reverse-mapped Excel (.xlsx) and CSV.

    Every call is a dashboard job (live stages + log at `/dashboard`, `/api/jobs/{job_id}`).
    """
    return _tracked_ingest(
        "file", os.path.basename(str(req.file_path).replace("\\", "/")) or str(req.file_path), str(req.file_path), req,
        {"source_type": "file", "data_key": req.file_path},
        f"file '{req.file_path}'",
    )

@app.post("/api/ingest", tags=["Ingestion"])
def unified_ingest(req: UnifiedIngestRequest):
    """
    **Unified Ingestion Endpoint**:
    Supports both `source_type='table'` (pass `db` and `table_name`) and `source_type='file'` (pass `file_path`).
    """
    src_type = req.source_type.strip().lower()
    if src_type == "table":
        if not req.db or not req.table_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Both 'db' and 'table_name' are required when source_type='table'."
            )
        return ingest_from_table(TableIngestRequest(
            db=req.db,
            table_name=req.table_name,
            tenant_id=req.tenant_id,
            org_id=req.org_id,
            product_name=req.product_name,
            user_id=req.user_id,
            transaction_id=req.transaction_id,
            save_location=req.save_location,
            process_code=req.process_code,
            transaction_type=req.transaction_type,
            theme_name=req.theme_name,
            wait=req.wait
        ))
    elif src_type == "file":
        file_target = req.file_path or req.data_key
        if not file_target:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="'file_path' (or 'data_key') is required when source_type='file'."
            )
        return ingest_from_file(FileIngestRequest(
            file_path=file_target,
            tenant_id=req.tenant_id,
            org_id=req.org_id,
            product_name=req.product_name,
            user_id=req.user_id,
            transaction_id=req.transaction_id,
            save_location=req.save_location,
            process_code=req.process_code,
            transaction_type=req.transaction_type,
            theme_name=req.theme_name,
            wait=req.wait
        ))
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid source_type '{req.source_type}'. Must be 'table' or 'file'."
        )

# =========================================================================
# Dashboard: file upload ingestion + live job logs
# =========================================================================

MAX_UPLOAD_BYTES = 5 * 1024 * 1024  # 5 MB
ALLOWED_UPLOAD_EXTENSIONS = {".xlsx", ".xls", ".csv"}
_ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")
_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9 _\-.()&]")
DASHBOARD_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard", "index.html")
_SSE_POLL_SECONDS = 0.3
_SSE_HEARTBEAT_SECONDS = 15


@app.middleware("http")
async def _reject_oversized_uploads(request: Request, call_next):
    """Reject obviously oversized uploads before the multipart body is parsed."""
    if request.url.path == "/api/ingest/upload":
        try:
            content_length = int(request.headers.get("content-length", "0"))
        except ValueError:
            content_length = 0
        # Allow some room for multipart boundaries and the form fields.
        if content_length > MAX_UPLOAD_BYTES + 64 * 1024:
            return JSONResponse(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                content={"detail": "File too large. Maximum allowed size is 5 MB."},
            )
    return await call_next(request)


def _safe_filename(name: str) -> str:
    base = os.path.basename((name or "").replace("\\", "/"))
    base = _UNSAFE_FILENAME_CHARS.sub("_", base).strip(" .")
    return base[:150]


def _new_transaction_id(prefix: str = "UPL") -> str:
    return f"TR_{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6].upper()}"


@app.get("/dashboard", include_in_schema=False)
def dashboard():
    """Ingestion dashboard UI (upload + live job logs)."""
    if not os.path.exists(DASHBOARD_FILE):
        raise HTTPException(status_code=404, detail="Dashboard UI not found.")
    return FileResponse(DASHBOARD_FILE, media_type="text/html")


@app.post("/api/ingest/upload", status_code=status.HTTP_202_ACCEPTED, tags=["Dashboard"])
async def upload_and_ingest(
    tenant_id: str = Form(..., description="Target Tenant ID", examples=["DM00005"]),
    org_id: str = Form(..., description="Organization ID", examples=["OM0000000002"]),
    file: UploadFile = File(..., description="Excel/CSV file (.xlsx, .xls, .csv), max 5 MB"),
):
    """
    **Upload a file and ingest it in the background**:
    1. Validates tenant/org, file type and size (max 5 MB).
    2. Saves the file under `uploads/<tenant>/<org>/` (same file name -> same data key, so re-uploads stay idempotent).
    3. Queues ingestion and returns immediately with a `job_id` (also used as the transaction ID).
    4. Follow progress live via `GET /api/jobs/{job_id}/stream` (SSE) or the `/dashboard` UI.
    """
    tenant_id = (tenant_id or "").strip()
    org_id = (org_id or "").strip()
    if not _ID_RE.match(tenant_id):
        raise HTTPException(status_code=400, detail="Invalid tenant_id. Use letters, digits, '_' or '-' (max 64).")
    if not _ID_RE.match(org_id):
        raise HTTPException(status_code=400, detail="Invalid org_id. Use letters, digits, '_' or '-' (max 64).")

    file_name = _safe_filename(file.filename)
    ext = os.path.splitext(file_name)[1].lower()
    if not file_name or ext not in ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{ext or file.filename}'. Allowed: {', '.join(sorted(ALLOWED_UPLOAD_EXTENSIONS))}.",
        )

    target_dir = os.path.join(job_manager.UPLOAD_DIR, tenant_id, org_id)
    os.makedirs(target_dir, exist_ok=True)
    final_path = os.path.join(target_dir, file_name)
    job_id = _new_transaction_id()
    tmp_path = os.path.join(target_dir, f".{job_id}.part")

    size = 0
    try:
        with open(tmp_path, "wb") as out:
            while True:
                chunk = await file.read(64 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="File too large. Maximum allowed size is 5 MB.")
                out.write(chunk)
        if size == 0:
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")

        try:
            job_manager.create_job(job_id, tenant_id, org_id, file_name, final_path, size)
        except job_manager.JobConflictError as e:
            raise HTTPException(status_code=409, detail=str(e))
        os.replace(tmp_path, final_path)
    finally:
        await file.close()
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    clog(f"API Request: Upload ingest '{file_name}' ({size} bytes) tenant={tenant_id} org={org_id} job={job_id}", "INFO")
    job_manager.submit(job_id, lambda: Job_Handler(
        source_type="file",
        data_key=final_path,
        tenant_id=tenant_id,
        org_id=org_id,
        transaction_id=job_id,
    ))
    return {
        "success": True,
        "message": f"File '{file_name}' accepted. Ingestion queued.",
        "job_id": job_id,
        "transaction_id": job_id,
        "status": "queued",
        "job_url": f"/api/jobs/{job_id}",
        "stream_url": f"/api/jobs/{job_id}/stream",
    }


def _sse(event: str, data: Any, event_id: Optional[int] = None) -> str:
    payload = json.dumps(data, ensure_ascii=False, default=str)
    head = f"id: {event_id}\n" if event_id is not None else ""
    return f"{head}event: {event}\ndata: {payload}\n\n"


_SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}


@app.get("/api/jobs", tags=["Dashboard"])
def list_jobs(limit: int = Query(100, ge=1, le=500)):
    """List recent upload ingestion jobs (newest first) with their current stage."""
    return {"jobs": job_manager.list_jobs(limit)}


@app.get("/api/jobs/stream", tags=["Dashboard"])
async def stream_all_jobs(request: Request):
    """
    SSE stream of job status/stage changes for the dashboard list.
    Sends a `snapshot` event first, then a `job` event on every change.
    """
    async def gen():
        last_seq = job_manager.current_global_seq()
        yield _sse("snapshot", {"jobs": job_manager.list_jobs(200)})
        idle = 0.0
        while not await request.is_disconnected():
            events, last = job_manager.global_events_since(last_seq)
            if events:
                for seq, summary in events:
                    yield _sse("job", summary, seq)
                last_seq = last
                idle = 0.0
            else:
                idle += _SSE_POLL_SECONDS
                if idle >= _SSE_HEARTBEAT_SECONDS:
                    yield ": ping\n\n"
                    idle = 0.0
            await asyncio.sleep(_SSE_POLL_SECONDS)

    return StreamingResponse(gen(), media_type="text/event-stream", headers=_SSE_HEADERS)


@app.get("/api/jobs/{job_id}", tags=["Dashboard"])
def get_job(job_id: str):
    """Job details including stage, result and the full captured log."""
    job_summary = job_manager.job_summary(job_id)
    if job_summary is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    events, _ = job_manager.job_events_since(job_id, 0)
    job_summary["logs"] = [e for e in (events or []) if e.get("type") == "log"]
    return job_summary


@app.get("/api/jobs/{job_id}/stream", tags=["Dashboard"])
async def stream_job(job_id: str, request: Request, last_id: int = Query(0, ge=0)):
    """
    SSE stream for one job: replays log history, then streams new `log` and
    `stage` events live, and ends with a `done` event. Resumes from the
    `Last-Event-ID` header (sent automatically by EventSource on reconnect).
    """
    if job_manager.get_job(job_id) is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    header_id = request.headers.get("last-event-id")
    if header_id and header_id.isdigit():
        last_id = max(last_id, int(header_id))

    async def gen():
        nonlocal last_id
        yield _sse("job", job_manager.job_summary(job_id))
        idle = 0.0
        while not await request.is_disconnected():
            events, _finished = job_manager.job_events_since(job_id, last_id)
            if events is None:
                break
            if not events and _finished:
                yield _sse("done", {"type": "done", "status": job_manager.job_summary(job_id)["status"]})
                return
            for ev in events:
                last_id = ev["id"]
                yield _sse(ev["type"], ev, ev["id"])
                if ev["type"] == "done":
                    yield _sse("job", job_manager.job_summary(job_id))
                    return
            if events:
                idle = 0.0
            else:
                idle += _SSE_POLL_SECONDS
                if idle >= _SSE_HEARTBEAT_SECONDS:
                    yield ": ping\n\n"
                    idle = 0.0
            await asyncio.sleep(_SSE_POLL_SECONDS)

    return StreamingResponse(gen(), media_type="text/event-stream", headers=_SSE_HEADERS)


if __name__ == "__main__":
    import uvicorn
    api_host = config.get('defaults', 'api_host', fallback='0.0.0.0')
    api_port = config.getint('defaults', 'api_port', fallback=8000)
    print(f"Starting Data Ingestion API server at http://{api_host}:{api_port}")
    print(f"Interactive Swagger documentation available at http://localhost:{api_port}/docs")
    print(f"Ingestion dashboard available at http://localhost:{api_port}/dashboard")
    uvicorn.run("api:app", host=api_host, port=api_port, reload=False)
