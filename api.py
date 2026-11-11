import os
import sys
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
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

class TableIngestRequest(BaseModel):
    db: str = Field(..., description="Source database name (e.g. 'dm00005')", example="dm00005")
    table_name: str = Field(..., description="Source table name to fetch and ingest", example="bsid_view")
    tenant_id: Optional[str] = Field(None, description="Target Tenant ID (defaults to config: DM00005)", example="DM00005")
    org_id: Optional[str] = Field(None, description="Organization ID (defaults to config: OM0000000002)", example="OM0000000002")
    product_name: Optional[str] = Field(None, description="Product Name (defaults to 'DTFOWS')", example="DTFOWS")
    user_id: Optional[str] = Field(None, description="User email/ID (defaults to config)", example="dilip@datatwin.ai")
    transaction_id: Optional[str] = Field(None, description="Custom Transaction ID (auto-generated if omitted)", example="TR_TABLE_INGEST_001")
    save_location: Optional[str] = Field(None, description="Output export folder (defaults to 'exports')", example="exports")

class FileIngestRequest(BaseModel):
    file_path: str = Field(..., description="Absolute local path or S3 key to the workbook (.xlsx, .csv)", example="C:\\VR3 PROJECT\\injection\\Audited_Books_GSTR-9_CUSAT_FY2022-23 copy.xlsx")
    tenant_id: Optional[str] = Field(None, description="Target Tenant ID (defaults to config: DM00005)", example="DM00005")
    org_id: Optional[str] = Field(None, description="Organization ID (defaults to config: OM0000000002)", example="OM0000000002")
    product_name: Optional[str] = Field(None, description="Product Name (defaults to 'DTFOWS')", example="DTFOWS")
    user_id: Optional[str] = Field(None, description="User email/ID (defaults to config)", example="dilip@datatwin.ai")
    transaction_id: Optional[str] = Field(None, description="Custom Transaction ID (auto-generated if omitted)", example="TR_FILE_INGEST_001")
    save_location: Optional[str] = Field(None, description="Output export folder (defaults to 'exports')", example="exports")

class UnifiedIngestRequest(BaseModel):
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
    Lists all public tables in the specified database along with their row counts.
    """
    try:
        conn = ec2confic(db)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.table_name, COALESCE(s.n_live_tup, 0) as row_count
                FROM information_schema.tables t
                LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
                WHERE t.table_schema = 'public'
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

@app.post("/api/ingest/table", tags=["Ingestion"])
def ingest_from_table(req: TableIngestRequest):
    """
    **Ingest data directly from a Database Table**:
    1. Fetches all records and columns from the specified `db` and `table_name`.
    2. Uses AI to identify transaction type, process code, and theme.
    3. Builds process and theme mapping for ALL columns in the table.
    4. Computes deterministic row checksums.
    5. Performs idempotent conditional UPSERT into `active_{process_code}`.
    6. Automatically exports reverse-mapped Excel (.xlsx) and CSV to `save_location`.
    """
    try:
        clog(f"API Request: Ingest from table '{req.db}.{req.table_name}'", "INFO")
        result = Job_Handler(
            source_type="table",
            source_db=req.db,
            source_table=req.table_name,
            tenant_id=req.tenant_id,
            org_id=req.org_id,
            product_name=req.product_name,
            user_id=req.user_id,
            transaction_id=req.transaction_id,
            save_location=req.save_location
        )
        return {
            "success": True,
            "message": f"Successfully ingested table '{req.table_name}' from database '{req.db}'",
            "data": result
        }
    except Exception as e:
        clog(f"API Error in ingest_from_table: {e}", "ERROR")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Table ingestion failed: {str(e)}"
        )

@app.post("/api/ingest/file", tags=["Ingestion"])
def ingest_from_file(req: FileIngestRequest):
    """
    **Ingest data from an Excel/CSV File**:
    1. Reads all sheets from local file path or S3 key.
    2. Runs AI classification, process cataloging, and theme mapping for each sheet.
    3. Performs idempotent conditional UPSERT into the target active table.
    4. Automatically exports reverse-mapped Excel (.xlsx) and CSV.
    """
    try:
        clog(f"API Request: Ingest from file '{req.file_path}'", "INFO")
        result = Job_Handler(
            source_type="file",
            data_key=req.file_path,
            tenant_id=req.tenant_id,
            org_id=req.org_id,
            product_name=req.product_name,
            user_id=req.user_id,
            transaction_id=req.transaction_id,
            save_location=req.save_location
        )
        return {
            "success": True,
            "message": f"Successfully ingested file '{req.file_path}'",
            "data": result
        }
    except Exception as e:
        clog(f"API Error in ingest_from_file: {e}", "ERROR")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"File ingestion failed: {str(e)}"
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
            save_location=req.save_location
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
            save_location=req.save_location
        ))
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid source_type '{req.source_type}'. Must be 'table' or 'file'."
        )

if __name__ == "__main__":
    import uvicorn
    api_host = config.get('defaults', 'api_host', fallback='0.0.0.0')
    api_port = config.getint('defaults', 'api_port', fallback=8000)
    print(f"Starting Data Ingestion API server at http://{api_host}:{api_port}")
    print(f"Interactive Swagger documentation available at http://localhost:{api_port}/docs")
    uvicorn.run("api:app", host=api_host, port=api_port, reload=False)
