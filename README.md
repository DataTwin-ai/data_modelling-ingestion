# Data Modelling & Ingestion

This pipeline reads financial data from an **Excel/CSV file** (on local disk or in S3) or from a **PostgreSQL table**. For each sheet, Gemini on Vertex AI classifies the data: it picks the transaction type, process code and theme. The pipeline then maps the columns to the standard schema and **upserts** the rows into the tenant's `active_<processcode>` table. Re-running the same data skips rows it has already loaded. When the run finishes, it exports the loaded data back to Excel and CSV files.

You can run it in three ways:

| Mode | Command | Use for |
| :--- | :--- | :--- |
| CLI / batch job | `python app.py [--Arg value ...]` | One-off runs, AWS Glue job |
| REST API | `python api.py` (or `python app.py --serve`) | Triggering ingestions over HTTP |
| Super Admin Correction | `python app.py --Process "Super Admin Correction" ...` | Re-classifying already ingested records |

---

## 1. Repository layout

| File | Purpose |
| :--- | :--- |
| `app.py` | The ingestion engine. `Job_Handler()` is the entry point. Also holds the S3 readers, AI calls, upsert logic, exports and logging. |
| `api.py` | FastAPI wrapper around `Job_Handler()` (file and table ingestion endpoints). |
| `config.ini` | All settings: environment, DB credentials, Gemini, default job arguments and **every SQL query** (`[querys]`). |
| `IDEMPOTENT_INGESTION_ARCHITECTURE.md` | Design notes for the checksum-based conditional UPSERT. |
| `generate_doc.py` | Builds a Word (.docx) architecture document with `python-docx`. |
| `Data Modelling & Ingestion.py` | Older, single-file version of the job (legacy Glue script). Use `app.py` instead. |
| `test.py` | Scratch script that creates a sample `BSID_ITEMS_converted.xlsx` test file. |

---

## 2. Prerequisites

- **Python 3.10+**
- **Network access to the PostgreSQL server** (the EC2 host in `config.ini`), with these databases:
  - `datatwin_db`: the common DB. It holds `process`, `theme_mapping`, `processcodes_knowledge` and so on.
  - One database per tenant, named after the tenant ID in lowercase (for example `dm00005`). The `active_*` tables and `transaction_log` are created there.
- **AWS credentials** with read access to S3. boto3 uses its normal credential chain: `aws configure`, environment variables or an instance role. The job reads from:
  - Tenant bucket `dt-<tenantid><suffix_s3>`, for example `dt-dm00005-dev`:
    - `VertexAI/credential.json`: the Google service account used for Gemini
    - `Prompt/Vertex ai/classification.txt`, `ERP_Generator.txt` and `prompt.EP.txt`: the AI prompts
    - `pretransaction/<file>`: the input files, when they are not found on local disk
  - `csvoistorage/configs/Date_Format_StdChart.csv`: the standard date-format chart
- **Vertex AI access** for the service account in the GCP project set in `[gemini]`.

### Install dependencies

There is no `requirements.txt` yet. Install the packages directly:

```bash
python -m venv .venv
```

```bash
.venv\Scripts\activate
```

```bash
pip install pandas numpy openpyxl regex psycopg2-binary==2.9.9 boto3 google-genai google-auth fastapi uvicorn pydantic python-docx
```

(On macOS/Linux, activate with `source .venv/bin/activate`.)

---

## 3. Configuration (`config.ini`)

All settings come from `config.ini`, which sits next to `app.py`. The file is read with `interpolation=None`, so `%s` and `%` in the SQL are written as-is.

### `[environment]`

| Key | Meaning |
| :--- | :--- |
| `current_env` | `development` or `production`. Chooses which section below supplies the DB, region and suffixes. |
| `log_level` | Informational only. Every `clog` level is always printed. |
| `log_file_prefix`, `log_extension` | Each run writes everything it prints (stdout and stderr) to `log_<YYYYMMDD_HHMMSS>.txt` in the project folder. |

### `[development]` / `[production]`

| Key | Meaning |
| :--- | :--- |
| `ec2_ip`, `port` | PostgreSQL host and port |
| `db_user`, `db_password` | PostgreSQL credentials |
| `region_name` | AWS region for the S3 client |
| `suffix_db` | DB suffix for the environment (`-DEV` or empty) |
| `suffix_s3` | Bucket suffix. The tenant bucket is `dt-<tenantid><suffix_s3>`. |

To switch environments, change `current_env`. Nothing else needs to change.

### `[gemini]`

| Key | Default |
| :--- | :--- |
| `project_id` | `datatwinai-dev` |
| `location` | `us-central1` |
| `model_name` | `gemini-2.5-flash` |

### `[defaults]`

These are the fallback job arguments, used when a CLI flag or API field is not given:

| Key | Meaning |
| :--- | :--- |
| `process` | `Data Modelling & Ingestion` (the normal flow) |
| `product_name` | e.g. `DTFOWS` |
| `tenant_id` | Tenant ID. It names the tenant DB and the S3 bucket. |
| `org_id` | Organisation ID written to `transaction_log` |
| `data_key` | Input file: a local absolute path **or** an S3 key (see §5) |
| `transaction_id` | Transaction ID. With several sheets, `_S1`, `_S2`, … is appended. |
| `user_id` | User recorded as `createdby` / `modifiedby` |
| `save_location` | Folder for exports (absolute path, or a path relative to the project) |
| `source_type` | `file` or `table` |
| `source_db`, `source_table` | Source used when `source_type = table` |
| `api_host`, `api_port` | Bind address for the API server (default `0.0.0.0:8000`) |

### `[querys]`

Every SQL statement the engine runs is defined here: schema checks, table creation, the conditional UPSERT, transaction-log updates, theme and process lookups, and exports. `app.py` refers to them as `QUERIES['<name>']`. To change SQL behaviour, edit it here, not in the code.

> ⚠️ `config.ini` currently contains real DB passwords and is committed to git. Consider moving the secrets to environment variables or AWS Secrets Manager.

---

## 4. Running it

### 4.1 CLI: run with the defaults from `config.ini`

```bash
python app.py
```

### 4.2 CLI: override with arguments (same flags as the AWS Glue job)

```bash
python app.py --Process "Data Modelling & Ingestion" --ProductName DTFOWS --TenantID DM00005 --OrgID OM0000000002 --DataKey "pretransaction/Vaigai_Notes_to_Accounts.xlsx" --TransactionID TRFS20260001 --UserID user@example.com
```

| Flag | Overrides |
| :--- | :--- |
| `--Process` | `defaults.process` |
| `--ProductName` | `defaults.product_name` |
| `--TenantID` | `defaults.tenant_id` |
| `--OrgID` | `defaults.org_id` |
| `--DataKey` | `defaults.data_key` |
| `--TransactionID` | `defaults.transaction_id` |
| `--UserID` | `defaults.user_id` |
| `--OldChanges`, `--NewChanges` | JSON used only by Super Admin Correction (§4.5) |

The CLI always uses `source_type` from `config.ini`. To ingest a **table** from the CLI, set `source_type = table` and fill in `source_db` and `source_table` in `[defaults]`.

### 4.3 AWS Glue

`app.py` runs as a Glue Python job. The flags above are passed as job parameters, with:

```
--additional-python-modules  google-genai,regex,psycopg2-binary==2.9.9
--extra-py-files             s3://gluescripts-demo/Layers/glue_dependencies.zip
```

The commented block at the bottom of `app.py` holds a full example.

### 4.4 REST API

```bash
python api.py
```

You can also start it with `python app.py --serve`, or with `uvicorn api:app --host 0.0.0.0 --port 8000`.

Interactive Swagger docs are at **http://localhost:8000/docs**.

| Method | Path | Body / params | Purpose |
| :--- | :--- | :--- | :--- |
| GET | `/` or `/api/health` | none | Status, environment, DB host |
| GET | `/api/tables?db=dm00005` | `db` query param | Lists the public tables in a DB, with row counts |
| POST | `/api/ingest/file` | `file_path` (required), plus optional fields | Ingests an Excel/CSV file |
| POST | `/api/ingest/table` | `db`, `table_name` (required), plus optional fields | Ingests a DB table |
| POST | `/api/ingest` | `source_type` = `file` or `table`, plus the matching fields | Single endpoint for both sources |

Optional fields on every ingest request: `tenant_id`, `org_id`, `product_name`, `user_id`, `transaction_id`, `save_location`. Any field you leave out falls back to `config.ini`.

Examples:

```bash
curl -X POST http://localhost:8000/api/ingest/file -H "Content-Type: application/json" -d "{\"file_path\": \"pretransaction/Vaigai_Notes_to_Accounts.xlsx\", \"tenant_id\": \"DM00005\", \"transaction_id\": \"TR_FILE_001\"}"
```

```bash
curl -X POST http://localhost:8000/api/ingest/table -H "Content-Type: application/json" -d "{\"db\": \"dm00005\", \"table_name\": \"bsid_view\", \"tenant_id\": \"DM00005\"}"
```

The response has the shape `{ "success": true, "message": ..., "data": { target_table, total_sheets, sheets, overall_results, exported_files, ... } }`.

### 4.5 Super Admin Correction

This mode re-classifies records that were already ingested under one process, theme or transaction type and moves them to another:

```bash
python app.py --Process "Super Admin Correction" --TenantID DM00005 --OrgID OM0000000002 --TransactionID TRFS20260001 --OldChanges "{\"ProcessCode\":\"FS\",\"ThemeName\":\"...\",\"TransactionType\":\"...\"}" --NewChanges "{\"ProcessCode\":\"TAXSYS\",\"ThemeName\":\"...\",\"TransactionType\":\"...\"}"
```

---

## 5. How a run works

```
source (file / table)
   │
   ▼
1. Acquire data
   • file : ReadAllSheetsS3(data_key) → one DataFrame per sheet
            data_key lookup order: local path → S3 key → pretransaction/<name> → <name>
            (an S3 file whose key is a local Windows path is also cached to that path)
   • table: SELECT * FROM <source_db>.<source_table> → one DataFrame
   │
   ▼  for each sheet / table
2. transaction_log ← "In Progress (<sheet>)"
3. Gemini classification   → TransactionType, ProcessCode, ThemeName
4. Process / theme metadata → creates new process + theme_mapping rows if the process is unknown
5. Column mapping           → source field names → standard target field names, validation
6. Row checksum (MD5 of business columns, normalised)
7. Conditional UPSERT into active_<processcode>
      new row            → INSERT  (createddatetime = now)
      same checksum      → SKIP    (no write)
      changed checksum   → UPDATE  (modifieddatetime = now, createddatetime kept)
   │
   ▼
8. Export to <save_location>:
      <name>_PROCxxxx.xlsx / _source_mapped.xlsx / _target_mapped.xlsx / .csv
9. transaction_log ← final status
```

Because of steps 6 and 7, **re-running the same file is safe**: identical rows are counted as *Skipped* and nothing is written for them. Each sheet logs a line like this one:

```
📊 Table 'active_fs' Bulk Ingestion Summary: Total: 28 | Inserted: 0 | Updated (changed): 0 | Skipped (identical): 28
```

See [IDEMPOTENT_INGESTION_ARCHITECTURE.md](IDEMPOTENT_INGESTION_ARCHITECTURE.md) for the details.

---

## 6. Outputs

| Output | Location |
| :--- | :--- |
| Ingested rows | `active_<processcode>` in the tenant DB |
| Run status | `transaction_log` in the tenant DB (`orgid`, `transactionid`) |
| Exports | `save_location` (default `exports/`; git-ignored) |
| Integrity-check exceptions | Uploaded to the tenant S3 bucket |
| Run log | `log_<timestamp>.txt` in the project folder (git-ignored) |

---

## 7. Troubleshooting

| Symptom | Check |
| :--- | :--- |
| `could not connect to server` | VPN or security group access to `ec2_ip:port`, and that `current_env` is correct |
| `NoCredentialsError` / `AccessDenied` from S3 | AWS credentials and region, and that bucket `dt-<tenant><suffix_s3>` exists |
| `NoSuchKey` for the input file | Use a local absolute path, or upload the file to `pretransaction/` in the tenant bucket |
| Gemini / Vertex errors | `VertexAI/credential.json` exists in the bucket, and the service account has Vertex AI access in `project_id` |
| `source_table is required` | Set `source_table` when `source_type = table` |
| `/api/ingest` with `source_type=file` and no `file_path` returns a 500 error instead of a 400 | Known bug: `api.py` reads `req.data_key`, which is not a field on `UnifiedIngestRequest`. Always send `file_path`. |
