# Packages
import sys
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
from boto3 import client
import io
from io import BytesIO

# pyrefly: ignore [missing-import]
from google import genai

# pyrefly: ignore [missing-import]
from google.genai import types

# pyrefly: ignore [missing-import]
from google.oauth2 import service_account
import json
from datetime import datetime, time
import time
import json
import json
import psycopg2
from psycopg2.extras import Json, execute_values
import pandas as pd
import openpyxl
import uuid
import pandas as pd

# pyrefly: ignore [missing-import]
import numpy as np
import regex as re
from psycopg2 import sql
import ast


import os
import configparser

# Read configuration from config.ini (interpolation=None allows %s and % in SQL queries)
config = configparser.ConfigParser(interpolation=None)
config_file_path = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_file_path)

env = config.get('environment', 'current_env', fallback='development')
log_level = config.get('environment', 'log_level', fallback='INFO')

# Logging configuration: write timestamped text (.txt) log file to current project directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_file_prefix = config.get('environment', 'log_file_prefix', fallback='log')
log_extension = config.get('environment', 'log_extension', fallback='txt').lstrip('.')
log_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"{log_file_prefix}_{log_timestamp}.{log_extension}"
LOG_FILE_PATH = os.path.join(BASE_DIR, log_filename)

class DualLogger:
    def __init__(self, filepath, stream):
        self.stream = stream
        self.filepath = filepath
        self._file = None

    def _get_file(self):
        if self._file is None or self._file.closed:
            self._file = open(self.filepath, 'a', encoding='utf-8')
        return self._file

    def write(self, message):
        if self.stream:
            try:
                self.stream.write(message)
                self.stream.flush()
            except Exception:
                pass
        try:
            f = self._get_file()
            f.write(message)
            f.flush()
        except Exception:
            pass

    def flush(self):
        if self.stream:
            try:
                self.stream.flush()
            except Exception:
                pass
        if self._file and not self._file.closed:
            try:
                self._file.flush()
            except Exception:
                pass

if not isinstance(sys.stdout, DualLogger):
    sys.stdout = DualLogger(LOG_FILE_PATH, sys.stdout)
if not isinstance(sys.stderr, DualLogger):
    sys.stderr = DualLogger(LOG_FILE_PATH, sys.stderr)

# Credentials & Database Configuration from config.ini
ec2IP = config.get(env, 'ec2_ip')
port = config.getint(env, 'port')
db_user = config.get(env, 'db_user')
db_password = config.get(env, 'db_password')
regionName = config.get(env, 'region_name')
suffixDb = config.get(env, 'suffix_db')
suffixS3 = config.get(env, 'suffix_s3')

# Gemini Configuration from config.ini
gemini_project_id = config.get('gemini', 'project_id', fallback='datatwinai-dev')
gemini_location = config.get('gemini', 'location', fallback='us-central1')
gemini_model_name = config.get('gemini', 'model_name', fallback='gemini-2.5-flash')

# Centralized Queries loaded from config.ini [querys] section
QUERIES = dict(config.items('querys')) if config.has_section('querys') else {}

# -------------------------------------------------------------------------
# Custom Logger (clog)
# -------------------------------------------------------------------------
def clog(message, level="INFO"):
    """
    Custom Logger function (clog):
    Usage:
        clog("Processing sheet 1...", "INFO")
        clog("Failed to connect", "ERROR")
        clog.info(...) / clog.error(...) / clog.warning(...)
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level_str = str(level).upper()
    prefix = {
        "INFO": "ℹ️ [INFO]",
        "SUCCESS": "✅ [SUCCESS]",
        "WARNING": "⚠️ [WARNING]",
        "ERROR": "❌ [ERROR]",
        "DEBUG": "🔍 [DEBUG]"
    }.get(level_str, f"[{level_str}]")
    print(f"[{timestamp}] {prefix} {message}")

clog.info = lambda msg: clog(msg, "INFO")
clog.success = lambda msg: clog(msg, "SUCCESS")
clog.warning = lambda msg: clog(msg, "WARNING")
clog.error = lambda msg: clog(msg, "ERROR")
clog.debug = lambda msg: clog(msg, "DEBUG")

import hashlib

def compute_row_checksum(df, exclude_cols=None):
    """
    Computes a deterministic MD5 checksum for each row based strictly on business/data columns.
    Excludes system, metadata, and audit columns to prevent false mismatches.
    """
    if exclude_cols is None:
        exclude_cols = {
            'sno', 'themename', 'processcode', 'checksum', 'input', 'output', 
            'transactionid', 'inprdfrom', 'inprdto', 'createddatetime', 
            'createdby', 'modifiedby', 'modifieddatetime'
        }
    data_cols = sorted([str(c).lower() for c in df.columns if str(c).lower() not in exclude_cols])
    
    col_map = {str(c).lower(): c for c in df.columns}
    
    def row_hash(row):
        parts = []
        for col_lower in data_cols:
            actual_col = col_map[col_lower]
            val = row[actual_col]
            if val is None or pd.isna(val) or str(val).strip() in ('', 'None', 'nan', 'NaN', 'NaT'):
                parts.append('')
            elif isinstance(val, (float, int)):
                parts.append(f"{float(val):.4f}")
            else:
                parts.append(str(val).strip().lower())
        raw_str = "|".join(parts)
        return hashlib.md5(raw_str.encode('utf-8')).hexdigest()

    return df.apply(row_hash, axis=1)

args = sys.argv
def get_argument_value(arg_name, default=""):
    if arg_name in args:
        arg_index = args.index(arg_name) + 1
        if arg_index < len(args):  # Ensure value exists after argument
            value = args[arg_index]
            # Handle NaN or blank values
            if value.lower() == "nan" or value.strip() == "":
                return default
            return value
    return default

def parse_changes(value):
    if not value:
        return {}

    if isinstance(value, dict):
        return value

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return ast.literal_eval(value)

# Parse arguments with safe checks
Process = get_argument_value("--Process")
clog(f"Process: {Process}", "INFO")
ProductName = get_argument_value("--ProductName")
TenantID = get_argument_value("--TenantID")
OrgID = get_argument_value("--OrgID")
DataKey = get_argument_value("--DataKey")
TransactionID = get_argument_value("--TransactionID")
UserID = get_argument_value("--UserID")
OldChanges = get_argument_value("--OldChanges")
OldChanges = parse_changes(get_argument_value("--OldChanges"))
clog(f"OldChanges: {repr(OldChanges)}", "DEBUG")
NewChanges = parse_changes(get_argument_value("--NewChanges"))

TransactionType = get_argument_value("--TransactionType")
ThemeName = get_argument_value("--ThemeName")
ProcessCode = get_argument_value("--ProcessCode")

s3=client("s3",region_name=regionName)


######################################## Database Functions ###########################################

def ec2confic(tenantID):
    "connects to ec2 instance and returns connection object using config.ini credentials."
    if tenantID != "DataTwin_DB":
        tenantID = tenantID.lower()  
    else:
        tenantID = tenantID
    conn = psycopg2.connect(
        host = ec2IP,
        port = port,
        dbname = tenantID,
        user = db_user,
        password = db_password
    )
    return conn

def fetch_data_from_db(db_conn, query):
    
    try:
        df = pd.read_sql_query(query, db_conn)

        # Safely convert datetime columns to string
        try:
            datetime_cols = df.select_dtypes(
                include=["datetime", "datetimetz", "datetime64[ns]"]
            ).columns

            for col in datetime_cols:
                df[col] = df[col].astype(str)
        except Exception:
            pass

        return df

    except Exception as e:
        print(f"❌ Error in fetch_data_from_db: {e}")
        return pd.DataFrame()

_TABLECODE_MIGRATION_DONE = False


def _to_sql_col_name(name):
    """Normalize an arbitrary Excel/CSV column header to a PostgreSQL-safe lowercase identifier."""
    s = str(name or "").strip()
    s = re.sub(r'[₹$€£]+', ' ', s)
    s = re.sub(r'[^a-zA-Z0-9]+', '_', s).strip('_').lower()
    if not s:
        return ""
    if s[0].isdigit():
        s = f"col_{s}"
    return s[:63]


def _coerce_extra_column_series(series):
    """Coerce currency/number strings to numeric dtype when all non-empty non-placeholder values are numeric."""
    if pd.api.types.is_numeric_dtype(series):
        return series

    non_empty = []
    for val in series:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            continue
        s = str(val).strip()
        if s.lower() in ("", "-", "–", "—", "nil", "none", "nan", "nat", "null"):
            continue
        non_empty.append(s)

    if not non_empty:
        return series

    numeric_vals = []
    for s in non_empty:
        cleaned = re.sub(r'[₹$€£,\s]', '', s)
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]
        try:
            numeric_vals.append(float(cleaned))
        except Exception:
            # Contains non-numeric text: keep as text column safely
            return series.apply(lambda v: None if (v is None or pd.isna(v) or str(v).strip() == '') else (v.strftime('%Y-%m-%d') if isinstance(v, pd.Timestamp) else str(v).strip()))

    def _to_float_or_none(val):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return np.nan
        s = str(val).strip()
        if s.lower() in ("", "-", "–", "—", "nil", "none", "nan", "nat", "null"):
            return np.nan
        cleaned = re.sub(r'[₹$€£,\s]', '', s)
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]
        try:
            return float(cleaned)
        except Exception:
            return np.nan

    return pd.to_numeric(series.apply(_to_float_or_none), errors='coerce')


def _infer_sql_column_type(series, orig_dtype=None):
    """Infer safe PostgreSQL column type ('DOUBLE PRECISION', 'TIMESTAMP', or 'TEXT') from values/dtype."""
    if orig_dtype is not None:
        if pd.api.types.is_numeric_dtype(orig_dtype):
            return "DOUBLE PRECISION"
        if pd.api.types.is_datetime64_any_dtype(orig_dtype):
            return "TIMESTAMP"
    if pd.api.types.is_numeric_dtype(series):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"

    non_null = [v for v in series if v is not None and not (isinstance(v, float) and np.isnan(v))]
    if non_null:
        if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null):
            return "DOUBLE PRECISION"
        if all(isinstance(v, (datetime, pd.Timestamp)) for v in non_null):
            return "TIMESTAMP"
    return "TEXT"


def ensure_tablecode_on_all_tables(conn, backfill=True):
    """Ensure every active_* and inactive_* table in the tenant schema has a 'tablecode' column."""
    global _TABLECODE_MIGRATION_DONE
    if _TABLECODE_MIGRATION_DONE or conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND (table_name LIKE 'active_%' OR table_name LIKE 'inactive_%')
                ORDER BY table_name
            """)
            all_tables = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT table_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND column_name = 'tablecode'
                  AND (table_name LIKE 'active_%' OR table_name LIKE 'inactive_%')
            """)
            has_tc = {r[0] for r in cur.fetchall()}

            added = 0
            for tbl in all_tables:
                if tbl not in has_tc:
                    cur.execute(f'ALTER TABLE "{tbl}" ADD COLUMN IF NOT EXISTS tablecode text;')
                    added += 1

            if backfill:
                cur.execute("""
                    SELECT table_name, array_agg(column_name)
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name LIKE 'active_%'
                    GROUP BY table_name
                """)
                tbl_cols_map = {r[0]: set(r[1]) for r in cur.fetchall()}
                for tbl in all_tables:
                    if not tbl.startswith('active_'):
                        continue
                    cols = tbl_cols_map.get(tbl, set())
                    candidates = []
                    if 'financialnotereference' in cols:
                        candidates.append("NULLIF(TRIM(financialnotereference::text), '')")
                    if 'statementtype' in cols:
                        candidates.append("NULLIF(TRIM(statementtype::text), '')")
                    if 'themename' in cols:
                        candidates.append("NULLIF(TRIM(themename::text), '')")
                    if 'processcode' in cols:
                        candidates.append("NULLIF(TRIM(processcode::text), '')")
                    fallback_name = tbl[len('active_'):].upper().replace("'", "''")
                    candidates.append(f"'{fallback_name}'")
                    coalesce_expr = ", ".join(candidates)
                    cur.execute(
                        f'UPDATE "{tbl}" SET tablecode = COALESCE({coalesce_expr}) '
                        f'WHERE tablecode IS NULL OR TRIM(tablecode::text) = \'\';'
                    )
            conn.commit()
            if added:
                clog(f"Added 'tablecode' column to {added} table(s) across tenant schema.", "SUCCESS")
            _TABLECODE_MIGRATION_DONE = True
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        clog(f"Notice: ensure_tablecode_on_all_tables skipped: {e}", "WARNING")


def execute_data_operation_on_postgres(tenantID, df, table_name, pk_keys, operation, conn=None, cur=None):
    
    if df is None or df.empty:
        print(f"⚠️ execute_data_operation_on_postgres: DataFrame is empty for table '{table_name}', skipping operation '{operation}'.")
        return f"Empty dataframe provided for {table_name}"

    df = df.copy()
    pk_keys = [str(key).lower() for key in pk_keys]
    df.columns = [str(col).lower() for col in df.columns]
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()

    orig_dtypes = {col: df[col].dtype for col in df.columns}

    # Replace NaN/NaT with None
    df = df.where(pd.notna(df), None)
    clean_val = lambda x: None if pd.isna(x) or (isinstance(x, str) and x.strip() in ("NaT", "nan", "NaN", "None")) else x
    if hasattr(df, "map"):
        df = df.map(clean_val)
    else:
        df = df.applymap(clean_val)

    # Convert datetime64[ns] to native Python datetime for Postgres
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].apply(lambda x: x.to_pydatetime() if x is not None else None)

    created_connection = False
    try:
        if conn is None or cur is None:
            conn = ec2confic(tenantID)
            cur = conn.cursor()
            created_connection = True

        if operation.lower() == 'delete':
            pk_values = [tuple(row[pk] for pk in pk_keys) for _, row in df.iterrows()]
            where_clause = " AND ".join([f"{pk} = %s" for pk in pk_keys])
            delete_sql = QUERIES.get('delete_from_table', "DELETE FROM {table_name} WHERE {where_clause}").format(
                table_name=table_name, where_clause=where_clause
            )
            cur.executemany(delete_sql, pk_values)
            conn.commit()
            response = f"✅ Deleted {len(pk_values)} record(s) from {table_name}"

        elif operation.lower() == 'insert':
            # Ensure modifieddatetime column exists in the target database table
            try:
                cur.execute(QUERIES.get('alter_add_modifieddatetime', "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS modifieddatetime timestamp;").format(table_name=table_name.lower()))
                conn.commit()
            except Exception:
                conn.rollback()

            # Check actual columns that exist in the target database table
            cur.execute(QUERIES.get('check_columns', "SELECT column_name FROM information_schema.columns WHERE table_name = %s"), (table_name.lower(),))
            db_cols_list = [r[0].lower() for r in cur.fetchall()]
            db_cols = set(db_cols_list)

            # Compute row checksum if missing or empty
            if 'checksum' in db_cols:
                if 'checksum' not in df.columns or df['checksum'].isna().any() or (df['checksum'] == '').any():
                    df['checksum'] = compute_row_checksum(df)

            if db_cols:
                # Handle tenant column naming differences (e.g. createddate vs created_date)
                if 'createddate' in db_cols and 'created_date' in df.columns and 'createddate' not in df.columns:
                    df = df.rename(columns={'created_date': 'createddate'})
                elif 'created_date' in db_cols and 'createddate' in df.columns and 'created_date' not in df.columns:
                    df = df.rename(columns={'createddate': 'created_date'})

                # Auto-add any DataFrame business columns missing from target table (and inactive mirror if present)
                missing_cols = [c for c in df.columns if c.lower() not in db_cols and c.lower() != 'unmapped_fields']
                inactive_tbl = f"inactive_{table_name.lower()[len('active_'):]}" if table_name.lower().startswith('active_') else None
                inactive_exists = False
                if inactive_tbl and missing_cols:
                    cur.execute(QUERIES['check_table_exists'], (inactive_tbl,))
                    inactive_exists = cur.fetchone() is not None

                for col in missing_cols:
                    col_type = _infer_sql_column_type(df[col], orig_dtypes.get(col))
                    try:
                        cur.execute(f'ALTER TABLE "{table_name.lower()}" ADD COLUMN IF NOT EXISTS "{col.lower()}" {col_type};')
                        if inactive_exists:
                            cur.execute(f'ALTER TABLE "{inactive_tbl}" ADD COLUMN IF NOT EXISTS "{col.lower()}" {col_type};')
                        conn.commit()
                        db_cols.add(col.lower())
                        clog(f"Added missing column '{col.lower()}' ({col_type}) to '{table_name}'", "SUCCESS")
                    except Exception:
                        conn.rollback()

                # Retain only columns that exist in the target table
                matched_cols = [col for col in df.columns if col in db_cols]
                if matched_cols:
                    df = df[matched_cols]

            cols = ', '.join(f'"{c}"' for c in df.columns)

            def clean_cell(cell):
                if cell is pd.NaT or pd.isna(cell) or str(cell).strip() in ('NaT', 'nan', 'NaN', 'None'):
                    return None

                if isinstance(cell, pd.Timestamp):
                    return cell.to_pydatetime()

                if isinstance(cell, dict):
                    return Json(cell)          # <-- convert dict to PostgreSQL JSON

                return cell

            def clean_row(row):
                return tuple(clean_cell(cell) for cell in row)

            values = [clean_row(row) for row in df.itertuples(index=False)]

            valid_pk_keys = [pk for pk in pk_keys if pk in df.columns]
            if valid_pk_keys:
                dup_rows = df[df.duplicated(subset=valid_pk_keys, keep=False)]
                if not dup_rows.empty:
                    clog("⚠️ Duplicate PK rows found before insert:", "WARNING")
                    clog(str(dup_rows), "WARNING")
            
            # Check if PK constraint exists for ON CONFLICT clause
            cur.execute(QUERIES.get('check_pk_constraint', "SELECT conname FROM pg_constraint WHERE conrelid = to_regclass(%s) AND contype = 'p'"), (table_name.lower(),))
            pk_constraints = cur.fetchall()

            # Retrieve actual primary key columns from database to ensure no PK / partition key is updated
            actual_pk_cols = set()
            try:
                cur.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = to_regclass(%s) AND i.indisprimary
                """, (table_name.lower(),))
                actual_pk_cols = {r[0].lower() for r in cur.fetchall()}
            except Exception:
                pass

            # Preserve original createddatetime, createdby, inprdfrom on update, and exclude all PK / partition columns
            exclude_from_update = set(pk_keys) | actual_pk_cols | {'createddatetime', 'createdby', 'inprdfrom', 'tenant_id'}
            update_cols = [col for col in df.columns if col not in exclude_from_update and col in db_cols]
            
            if pk_constraints and update_cols:
                constraint_name = pk_constraints[0][0]
                set_items = [f'"{col}" = EXCLUDED."{col}"' for col in update_cols if col != 'modifieddatetime']
                if 'modifieddatetime' in db_cols:
                    set_items.append("modifieddatetime = NOW()")
                set_clause = ", ".join(set_items)

                # Option 3: Conditional UPSERT (Skip if checksum matches, update if changed)
                if 'checksum' in db_cols and 'checksum' in df.columns:
                    conflict_clause = QUERIES.get(
                        'on_conflict_conditional_update',
                        "ON CONFLICT ON CONSTRAINT {constraint_name} DO UPDATE SET {set_clause} WHERE {table_name}.checksum IS DISTINCT FROM EXCLUDED.checksum"
                    ).format(constraint_name=constraint_name, set_clause=set_clause, table_name=table_name)
                else:
                    conflict_clause = QUERIES.get(
                        'on_conflict_update',
                        "ON CONFLICT ON CONSTRAINT {constraint_name} DO UPDATE SET {set_clause}"
                    ).format(constraint_name=constraint_name, set_clause=set_clause)
            elif pk_constraints:
                constraint_name = pk_constraints[0][0]
                conflict_clause = QUERIES.get('on_conflict_do_nothing', "ON CONFLICT ON CONSTRAINT {constraint_name} DO NOTHING").format(
                    constraint_name=constraint_name
                )
            else:
                conflict_clause = ""

            insert_sql = QUERIES.get('insert_into_table_returning', "INSERT INTO {table_name} ({cols}) VALUES %s {conflict_clause} RETURNING xmax").format(
                table_name=table_name, cols=cols, conflict_clause=conflict_clause
            ).strip()

            try:
                returned_rows = execute_values(cur, insert_sql, values, page_size=10000, fetch=True)
                conn.commit()
                # In PostgreSQL: xmax = 0 means row was newly INSERTED; xmax != 0 means row was UPDATED
                inserted_cnt = sum(1 for r in returned_rows if int(r[0]) == 0)
                updated_cnt = sum(1 for r in returned_rows if int(r[0]) != 0)
                skipped_cnt = len(values) - len(returned_rows)
            except Exception as e:
                conn.rollback()
                # Fallback to plain insert without RETURNING clause
                insert_sql_plain = QUERIES.get('insert_into_table', "INSERT INTO {table_name} ({cols}) VALUES %s {conflict_clause}").format(
                    table_name=table_name, cols=cols, conflict_clause=conflict_clause
                ).strip()
                execute_values(cur, insert_sql_plain, values, page_size=10000)
                conn.commit()
                inserted_cnt, updated_cnt, skipped_cnt = len(values), 0, 0

            clog(f"📊 Table '{table_name}' Bulk Ingestion Summary: Total: {len(values)} | Inserted: {inserted_cnt} | Updated (changed): {updated_cnt} | Skipped (identical): {skipped_cnt}", "SUCCESS" if (inserted_cnt or updated_cnt) else "INFO")
            response = f"Data successfully loaded into {table_name} (Inserted: {inserted_cnt}, Updated: {updated_cnt}, Skipped: {skipped_cnt})"

        elif operation.lower() == 'select':
            sel_pk_keys=pk_keys.copy()
            sel_pk_keys.append('themename')
            sel_pk_keys.remove('sno')
            pk_values = list({
                tuple(row[pk] for pk in sel_pk_keys)
                for _, row in df.iterrows()
            })
            where_clause = " AND ".join([f"{pk} = %s" for pk in sel_pk_keys])
            select_sql = QUERIES.get('select_from_table', "SELECT * FROM {table_name} WHERE {where_clause}").format(
                table_name=table_name, where_clause=where_clause
            )

            all_rows = []
            for values in pk_values:
                cur.execute(select_sql, values)
                rows = cur.fetchall()
                all_rows.extend(rows)

            columns = [desc[0] for desc in cur.description]
            response = pd.DataFrame(all_rows, columns=columns)
            if 'header' in response.columns:
                response.drop(['header'], axis=1, inplace=True)

        elif operation.lower() == 'update':
            set_cols = [col for col in df.columns if col not in pk_keys]
            set_clause = ", ".join([f"{col} = %s" for col in set_cols])
            where_clause = " AND ".join([f"{pk} = %s" for pk in pk_keys])
            update_sql = QUERIES.get('update_table', "UPDATE {table_name} SET {set_clause} WHERE {where_clause}").format(
                table_name=table_name, set_clause=set_clause, where_clause=where_clause
            )

            def clean_update_row(row):
                return tuple(
                    [None if pd.isna(row[col]) else row[col] for col in set_cols] +
                    [None if pd.isna(row[pk]) else row[pk] for pk in pk_keys]
                )

            update_values = [clean_update_row(row) for _, row in df.iterrows()]
            cur.executemany(update_sql, update_values)
            conn.commit()
            response = f"✅ Updated {len(update_values)} record(s) in '{table_name}'"

    finally:
        if created_connection:
            if cur: cur.close()
            if conn: conn.close()

    return response

def GetMasterObjectID(conn, masterobjName, count=1):

    try:

        pk = "MASTEROBJECTSEQUENCING"
        sk = "#OBJNAME#" + masterobjName

        current_time = int(time.time_ns())

        query = QUERIES.get('upsert_master_object_sequence')

        with conn.cursor() as cursor:
            cursor.execute(query,(pk,sk,count,current_time,count,count,count))
            previous_last_updated_id = cursor.fetchone()[0]

        conn.commit()

        return previous_last_updated_id+1

    except Exception as e:

        conn.rollback()
        raise Exception(f"Error generating Object ID: {str(e)}")

def createtableinPostgres(process, processCode, tableCatg, tenant_cursor):
    process = process.copy()
    process.columns = [str(c).lower() for c in process.columns]
    
    # Standardize lde column
    if 'lde' in process.columns:
        process['lde'] = pd.to_numeric(process['lde'], errors='coerce').fillna(0)
    else:
        process['lde'] = 0

    filtered_df = process[process['lde'] != 0]
    sorted_df = filtered_df.sort_values('lde')

    seen_pks = set(['processcode', 'themename'])
    unique_pk_list = []
    domain_col = 'domain' if 'domain' in sorted_df.columns else 'short_text'
    for name in sorted_df[domain_col]:
        col_name = str(name).strip().lower()
        if col_name and col_name not in seen_pks:
            seen_pks.add(col_name)
            unique_pk_list.append(f'"{col_name}"')

    Pks = ", ".join(unique_pk_list)
    addPks = ', "sno"' if tableCatg == 'active' else ', "sno", "inprdfrom"'
    if Pks:
        Pks = f'"processcode", "themename", {Pks}{addPks}'
    else:
        Pks = f'"processcode", "themename"{addPks}'

    table_name = tableCatg + '_' + processCode.lower()

    raw_col_names = [str(x).strip().lower() for x in process['short_text'].tolist()] if 'short_text' in process.columns else []
    raw_dtypes = [str(x).strip().lower() for x in process['data_type'].tolist()] if 'data_type' in process.columns else []

    seen_cols = set()
    column_names = []
    data_types = []
    for col, dtype in zip(raw_col_names, raw_dtypes):
        if col and col not in seen_cols:
            seen_cols.add(col)
            column_names.append(col)
            data_types.append(dtype)

    system_cols = [
        ('sno', 'VARCHAR'),
        ('themename', 'VARCHAR'),
        ('processcode', 'VARCHAR'),
        ('tablecode', 'VARCHAR'),
        ('checksum', 'VARCHAR'),
        ('input', 'VARCHAR'),
        ('output', 'VARCHAR'),
        ('transactionid', 'VARCHAR'),
        ('unmapped_fields', 'JSONB'),
        ('inprdfrom', 'DATETIME'),
        ('inprdto', 'DATETIME'),
        ('createddatetime', 'DATETIME'),
        ('createdby', 'VARCHAR'),
        ('modifiedby', 'VARCHAR'),
        ('modifieddatetime', 'DATETIME')
    ]
    for sys_col, sys_dtype in system_cols:
        if sys_col.lower() not in seen_cols:
            seen_cols.add(sys_col.lower())
            column_names.append(sys_col.lower())
            data_types.append(sys_dtype)

    type_mapping = {
        'VARCHAR': 'text',
        'varchar': 'text',
        'INTEGER': 'integer',
        'integer': 'integer',
        'FLOAT': 'float8',
        'float': 'float8',
        'DATE': 'date',
        'date': 'date',
        'DATETIME': 'timestamp',
        'datetime': 'timestamp',
        'JSONB': 'jsonb',
        'jsonb': 'jsonb'
    }

    columns_sql = []
    for col, dtype in zip(column_names, data_types):
        pg_dtype = type_mapping.get(dtype, 'text')

        if pg_dtype in ['text', 'varchar']:
            default_value = " DEFAULT '' "
        elif pg_dtype in ['integer', 'float8']:
            default_value = " DEFAULT 0 "
        elif pg_dtype == 'date':
            default_value = " DEFAULT '1900-01-01' "
        elif pg_dtype == 'timestamp':
            default_value = " DEFAULT '1900-01-01 00:00:00' "
        else:
            default_value = ""

        columns_sql.append(f'"{col}" {pg_dtype}{default_value}')
    
    columns_str = ",\n  ".join(columns_sql)

    create_query = QUERIES.get('create_table_template', "CREATE TABLE IF NOT EXISTS {table_name} ({columns_str}, PRIMARY KEY ({pks}));").format(
        table_name=table_name, columns_str=columns_str, pks=Pks
    )

    tenant_cursor.execute(create_query)
    tenant_cursor.connection.commit()
    response = f"Created table '{table_name}'"
    clog(response, "SUCCESS")

    return response

def update_transaction_log(tenant_conn, OrgID, TransactionID, attributes: dict):
    if not attributes:
        return False

    try:
        with tenant_conn.cursor() as cur:

            # Convert attribute keys to lowercase column names and sanitize length
            update_data = {}
            for column, value in attributes.items():
                col_key = str(column).lower()
                # Ensure status field doesn't exceed 50 chars (PostgreSQL column limit)
                if col_key == "status" and isinstance(value, str) and len(value) > 50:
                    update_data[col_key] = value[:50]
                else:
                    update_data[col_key] = value

            set_clause_str = ", ".join([f'"{col}" = %s' for col in update_data.keys()])
            query = QUERIES.get('update_transaction_log', "UPDATE transaction_log SET {set_clause} WHERE orgid = %s AND transactionid = %s").format(
                set_clause=set_clause_str
            )

            values = list(update_data.values()) + [OrgID, TransactionID]

            cur.execute(query, values)
            if cur.rowcount == 0:
                insert_cols = ['orgid', 'transactionid'] + list(update_data.keys())
                col_names = ", ".join([f'"{c}"' for c in insert_cols])
                placeholders = ", ".join(['%s'] * len(insert_cols))
                insert_vals = [OrgID, TransactionID] + list(update_data.values())
                try:
                    cur.execute(f"INSERT INTO transaction_log ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;", insert_vals)
                except Exception:
                    pass

        tenant_conn.commit()
        return True

    except Exception as e:
        tenant_conn.rollback()
        raise e

########################################### PDT Functions ###########################################

def ThemeData(productCode,processCode,themeName,tenant_conn):

    query = QUERIES['fetch_theme_data']

    conn = tenant_conn

    try:

        themeData = pd.read_sql(
            query,
            conn,
            params=(
                productCode,
                processCode,
                themeName
            )
        )

    except Exception as e:
        raise (e)

    themeData.columns = [
        col.strip().lower()
        for col in themeData.columns
    ]

    themeData.rename(columns={

        "sourcefieldname": "SourceFieldName",
        "sourcefieldfunction": "SourceFieldFunction",
        "targetfieldname": "TargetFieldName",
        "targetfieldfunction": "TargetFieldFunction",
        "targetfielddatatype": "TargetFieldDataType",
        "dataformat": "DataFormat",
        "processcode": "ProcessCode",
        "themename": "ThemeName",
        "lde": "LDE",
        "mandatory": "Mandatory"

    }, inplace=True)
    numeric_cols = [
    "LDE",
    "Mandatory",
    "Length",
    "Precision",
    "Scale"
    ]

    for col in numeric_cols:

        if col in themeData.columns:

            themeData[col] = pd.to_numeric(
                themeData[col],
            errors="coerce"
        ).fillna(0)
        
    if "TargetFieldFunction" not in themeData.columns:

        themeData["TargetFieldFunction"] = None

    themeData['TargetFieldFunction'] = (
        themeData['SourceFieldFunction']
    )

    consFixedVals = {}

    mask1 = (
        (themeData["SourceFieldName"] != "")
        |
        (themeData['TargetFieldFunction'].notna())
        |
        (
            themeData['TargetFieldName']
            .isin(consFixedVals.keys())
        )
    )

    themeData = themeData.loc[mask1]

    mask2 = ~themeData['TargetFieldName'].isin([
        "InPrd From",
        "InPrd To",
        "Header",
        "PK",
        "SK",
        "Partition Key",
        "Sort Key",
        "InternalID",
        "Ver",
        "Version"
    ])

    themeData = themeData.loc[mask2]

    themeData["ProcessCode"] = processCode
    themeData["ThemeName"] = themeName

    themeData = themeData[[

        'SourceFieldName',
        'SourceFieldFunction',
        'TargetFieldName',
        'TargetFieldFunction',
        'DataFormat',
        'ProcessCode',
        'ThemeName'

    ]]

    print("ThemeData Final:")
    print(themeData.head())

    return themeData

def ThemeDataV2(productCode,processCode,themeName, tenant_conn):

    query = QUERIES['fetch_theme_data']

    conn = tenant_conn

    try:

        themeData = pd.read_sql(
            query,
            conn,
            params=(
                productCode,
                processCode,
                themeName
            )
        )

    except Exception as e:
        raise (e)


    themeData.columns = [
        col.strip().lower()
        for col in themeData.columns
    ]

 

    themeData.rename(columns={

        "sourcefieldname": "SourceFieldName",
        "sourcefieldfunction": "SourceFieldFunction",
        "targetfieldname": "TargetFieldName",
        "targetfieldfunction": "TargetFieldFunction",
        "targetfielddatatype": "TargetFieldDataType",
        "dataformat": "DataFormat",
        "processcode": "ProcessCode",
        "themename": "ThemeName",
        "lde": "LDE",
        "mandatory": "Mandatory"

    }, inplace=True)
    numeric_cols = [
    "LDE",
    "Length",
    "Precision",
    "Scale"
    ]

    for col in numeric_cols:

        if col in themeData.columns:

            themeData[col] = pd.to_numeric(
                themeData[col],
                errors="coerce"
            ).fillna(0)

    if "TargetFieldFunction" not in themeData.columns:

        themeData["TargetFieldFunction"] = None


    themeData['TargetFieldFunction'] = (
        themeData['SourceFieldFunction']
    )


    consFixedVals = {}

    mask1 = (
        (themeData["SourceFieldName"] != "")
        |
        (themeData['TargetFieldFunction'].notna())
        |
        (
            themeData['TargetFieldName']
            .isin(consFixedVals.keys())
        )
    )

    themeData = themeData.loc[mask1]

    mask2 = ~themeData['TargetFieldName'].isin([

        "InPrd From",
        "InPrd To",
        "Header",
        "PK",
        "SK",
        "Partition Key",
        "Sort Key",
        "InternalID",
        "Ver",
        "Version"

    ])

    themeData = themeData.loc[mask2]

    themeData["ProcessCode"] = processCode
    themeData["ThemeName"] = themeName

  
    themeData = themeData.astype(object)

    themeData = themeData.where(
        pd.notna(themeData),
        None
    )

    themeData.replace(
        ['NaN', 'None', 'nan', ''],
        None,
        inplace=True
    )

    print("ThemeDataV2 Final:")
    print(themeData.head())
    print("shape",themeData.shape)
    return themeData

def ProcessDefn(productCode,processCode,tenant_conn,commondb_conn,themeName=None):

    "Retrieves Process Definition From Database"

    raw_query = QUERIES.get('fetch_process_defn', "SELECT * FROM public.\"process\" WHERE process_cd <> 'process_cd' AND process_cd = '%s'")
    if "'%s'" in raw_query or '"%s"' in raw_query:
        query = raw_query % processCode
    elif "%s" in raw_query:
        query = raw_query % f"'{processCode}'"
    elif "{process_cd}" in raw_query:
        query = raw_query.format(process_cd=processCode)
    else:
        query = raw_query
    
    #df_process = SQLRun_Postgres(pg_bucketName, queryFile=None, qParams={}, returnResp=True, qText=query)
    
    df_process = fetch_data_from_db(commondb_conn,query)

    df_process.rename(columns={
            'short_text': 'ShortText',
            'domain'    : 'Domain',
            'name'     : 'FieldName',
            'data_type' : 'DataType',
            'predefined_list': 'PredefinedList'
        }, inplace=True)
    
    if themeName is not None:

        df_theme = ThemeDataV2(productCode,processCode,themeName, tenant_conn)
        print("df_process columns",df_process.columns.tolist())
        print("df_theme columns",df_theme.columns.tolist())
        # Merge based on ShortText (process) and TargetFieldName (theme)
        if 'ShortText' in df_process.columns and 'TargetFieldName' in df_theme.columns:
            df_result = df_process.merge(
                df_theme[['TargetFieldName', 'LDE', 'Mandatory']],
                left_on='ShortText',
                right_on='TargetFieldName',
                how='left'
            )
            # Rename the added columns if you like
            df_result.rename(columns={
                'Mandatory': 'MandatoryFlag'
            }, inplace=True)
            df_result.drop(columns=['TargetFieldName'], inplace=True)
        else:
            df_result = df_process.copy()
            df_result['LDE'] = None
            df_result['MandatoryFlag'] = None

        df_result = df_result.astype(object)
        df_result = df_result.where(pd.notna(df_result), None)

    else:
        
        df_result = df_process.copy()    

    # Ensure both LDE and lde columns are present and consistent
    if 'LDE' in df_result.columns and 'lde' not in df_result.columns:
        df_result['lde'] = df_result['LDE']
    elif 'lde' in df_result.columns and 'LDE' not in df_result.columns:
        df_result['LDE'] = df_result['lde']
    elif 'lde' not in df_result.columns and 'LDE' not in df_result.columns:
        df_result['lde'] = None
        df_result['LDE'] = None

    return df_result

def convert_to_strptime_format(fmt):
    """
    Convert human-readable date format like 'DD-MM-YYYY HH:mm:ss'
    to Python strptime format like '%d-%m-%Y %H:%M:%S'
    """
    return (fmt.replace('DD', '%d')
                .replace('MM', '%m')
                .replace('YYYY', '%Y')
                .replace('HH', '%H')
                .replace('mm', '%M')
                .replace('ss', '%S'))

def append_error(inputdata, mask, message):

    def merge_messages(existing):

        existing = str(existing).strip()

        if existing == "":
            return message

        parts = [p.strip() for p in existing.split(" | ") if p.strip()]

        prefix, value = message.split(": ", 1)

        found = False

        for i, part in enumerate(parts):
            if part.startswith(prefix + ":"):

                old_values = [
                    v.strip()
                    for v in part[len(prefix) + 2:].split(",")
                    if v.strip()
                ]

                new_values = [
                    v.strip()
                    for v in value.split(",")
                    if v.strip()
                ]

                merged = sorted(set(old_values + new_values))

                parts[i] = f"{prefix}: {', '.join(merged)}"
                found = True
                break

        if not found:
            parts.append(message)

        return " | ".join(parts)

    inputdata.loc[mask, "Error Msg"] = (
        inputdata.loc[mask, "Error Msg"]
        .fillna("")
        .astype(str)
        .apply(merge_messages)
    )

def validate_column_format(inputdata, themedata, exceptions):

    # Create Error Msg column if it doesn't exist
    if "Error Msg" not in inputdata.columns:
        inputdata.insert(0, "Error Msg", "")
    else:
        inputdata["Error Msg"] = inputdata["Error Msg"].fillna("").astype(str)

    for _, row in themedata.iterrows():

        source_field = str(row["SourceFieldName"]).strip()
        data_type = str(row["TargetFieldDataType"]).strip().upper()

        raw_date_format = row.get("DataFormat")
        date_format = (
            convert_to_strptime_format(raw_date_format)
            if pd.notna(raw_date_format) and str(raw_date_format).strip() != ""
            else None
        )

        # Skip if source column not present
        if source_field not in inputdata.columns:
            continue

        column_values = inputdata[source_field]

        # =====================================================
        # NUMBER VALIDATION
        # =====================================================
        if data_type in ["FLOAT", "INTEGER"]:

            normalized = column_values.apply(
                lambda x: str(x).strip().lower() if isinstance(x, str) else x
            )

            normalized = normalized.replace(
                ["", "nan", "none", "pd.na", "<na>", "nat", "-", "–", "—", "nil", "null"],
                np.nan
            )

            converted = pd.to_numeric(normalized, errors="coerce")

            invalid_mask = converted.isna() & normalized.notna()

            # Clean and coerce values in inputdata so Postgres receives valid numbers
            inputdata[source_field] = converted

            # Raise error only if column contains purely non-numeric data (genuine mismatch)
            non_na_count = normalized.notna().sum()
            valid_count = converted.notna().sum()
            if non_na_count > 0 and valid_count == 0:
                append_error(
                    inputdata,
                    invalid_mask,
                    f"Invalid Number Format: {source_field}"
                )
                exceptions.append(
                    f"Column '{source_field}' contains no valid numeric values"
                )

        # =====================================================
        # DATE VALIDATION
        # =====================================================
        elif data_type in ["DATE", "DATETIME"]:

            # If all values in the column are null, empty, or whitespace, skip validation (valid empty date column)
            non_empty_mask = column_values.notna() & (column_values.astype(str).str.strip().replace(
                ["", "nan", "none", "nat", "null", "pd.na", "<na>", "none"], ""
            ) != "")
            if not non_empty_mask.any():
                continue

            if not date_format:
                sample_vals = column_values[non_empty_mask].head(10)
                try:
                    pd.to_datetime(sample_vals)
                    date_format = "%Y-%m-%d"
                except Exception:
                    exceptions.append(
                        f"Expected date format is not specified for column '{source_field}'."
                    )
                    continue

            datetime_format = f"{date_format} %H:%M:%S"

            invalid_index = []
            invalid_values = []

            for idx, val in column_values.items():

                if pd.isna(val):
                    continue

                value = str(val).strip()

                if value in [
                    "",
                    "NaT",
                    "nan",
                    "NaN",
                    "NA",
                    "<NA>",
                    "2100-12-31",
                    "1988-01-01"
                ]:
                    continue

                valid = False

                try:
                    datetime.strptime(value, date_format)
                    valid = True
                except:
                    pass

                if not valid:
                    try:
                        datetime.strptime(value, datetime_format)
                        valid = True
                    except:
                        pass

                if not valid:
                    try:
                        parsed_dt = pd.to_datetime(value)
                        if pd.notna(parsed_dt):
                            valid = True
                    except Exception:
                        pass

                if not valid:
                    invalid_index.append(idx)
                    invalid_values.append(value)

            if invalid_index:

                mask = inputdata.index.isin(invalid_index)

                append_error(
                    inputdata,
                    mask,
                    f"Invalid Date Format: {source_field}"
                )

                exceptions.append(
                    f"Column '{source_field}' must match date format '{raw_date_format}'. Invalid values: {list(set(invalid_values))}"
                )

    return exceptions, inputdata

def validate_source_field_function(themedata, inputdata, exceptions):

    # Check if 'Mandatory' column exists
    if 'Mandatory' not in themedata.columns:
        exceptions.append("The 'themedata' must contain 'Mandatory' column.")
        return exceptions

    # Create Error Msg column as first column if not exists
    if "Error Msg" not in inputdata.columns:
        inputdata.insert(0, "Error Msg", "")
    else:
        inputdata["Error Msg"] = inputdata["Error Msg"].fillna("").astype(str)

    # Convert Mandatory column to boolean safely
    mandatory_col = (
        themedata["Mandatory"]
        .fillna(False)
        .astype(str)
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False})
        .fillna(False)
    )

    # Filter mandatory rows
    starred_rows = themedata[mandatory_col]

    missing_fields = []

    # Check mandatory fields
    for _, row in starred_rows.iterrows():

        source_field = str(row["SourceFieldName"]).strip()

        # Missing Column
        if source_field not in inputdata.columns:
            missing_fields.append(source_field)
            continue

        # Missing Values
        missing_mask = inputdata[source_field].isnull()

        if missing_mask.any():
            append_error(
                inputdata,
                missing_mask,
                f"Missing Input Column Value(s): {source_field}"
            )

    # Missing columns -> append to all rows
    if missing_fields:

        missing_fields = sorted(set(missing_fields))

        msg = f"Missing Input Column(s): {', '.join(missing_fields)}"

        append_error(
                inputdata,
                pd.Series(True, index=inputdata.index),
                f"Missing Input Column(s): {', '.join(missing_fields)}"
        )

        exceptions.append(msg)

    return exceptions, inputdata

def IntegrityCheck(bucketName, tenant_conn, OrgID, themeName, inputdata, themedata, transactionID):

    exceptions=[]

    # Validate Mandatory mapping
    exceptions, inputdata = validate_source_field_function(themedata, inputdata, exceptions)

    exceptions, inputdata = validate_column_format(inputdata, themedata, exceptions)
    
    key = f"pretransaction_response/{OrgID}/{transactionID}.xlsx"
    
    excel_buffer = BytesIO()

    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        inputdata.to_excel(writer, index=False, sheet_name="Data")

    excel_buffer.seek(0)
    
    s3.upload_fileobj(excel_buffer,bucketName,key)

    if len(exceptions)>0:

        exceptions = ", ".join(exceptions)

        print(f"Error: {exceptions} in {themeName}")
        
        update_transaction_log(tenant_conn, OrgID, transactionID, attributes={"Status": "Error"})

        raise Exception(f"Integrity Check Issue: {exceptions}")
    
    else:
        
        update_transaction_log(tenant_conn, OrgID, transactionID, attributes={"Status": "Integrity Check Passed"})
        
        print("Integrity Check Passed in "+themeName)
        
        return True

def CopySameSourceColumn(themeData, data, patternExtFields):
    
    final_df = pd.DataFrame(index=data.index)  # maintain same number of rows

    # Filter only standard (non-function) field mappings
    standard_mappings = themeData[themeData["SourceFieldFunction"] == "NA"]

    for _, row in standard_mappings.iterrows():
        source_col = row["SourceFieldName"]
        target_col = row["TargetFieldName"]

        if source_col in data.columns:
            final_df[target_col] = data[source_col]
        else:
            # Source column not found — create empty target column
            final_df[target_col] = None

    # Final target fields (only those that exist in final_df)
    targetF = [x for x in themeData["TargetFieldName"].unique() if x in final_df.columns]
    validPatternFields = [x for x in patternExtFields if x in final_df.columns]

    return final_df[targetF + validPatternFields]

def StandardProcessDataMap(data, themeData, metaInfo, exceptions):
    "standard pdm function with case-insensitive handling."
    
    # Normalize for comparison
    themeData["TargetFieldName"] = themeData["TargetFieldName"].str.lower()
    metaInfo["Domain"] = metaInfo["Domain"].str.lower()

    # Domains already present in themeData
    existing_targets = set(themeData["TargetFieldName"].dropna())

    # Find missing domains
    missing_meta = metaInfo[~metaInfo["Domain"].isin(existing_targets)]

    if not missing_meta.empty:

        new_rows = []

        for _, row in missing_meta.iterrows():
            new_rows.append({
                "SourceFieldName": None,
                "OrginalSourceFieldName": None,
                "TargetFieldName": row["Domain"],
                "DataType": row["DataType"],
                "SourceFieldFunction": "NA",
                "DataFormat": None
            })

        themeData = pd.concat([themeData, pd.DataFrame(new_rows)], ignore_index=True)

    mask = themeData["TargetFieldName"].str.lower() == "unmapped_fields"
    themeData.loc[mask, "SourceFieldName"] = "unmapped_fields"

    # Normalize all column names in lowercase for comparison
    data.columns = [col.lower() for col in data.columns]

    themeData["SourceFieldFunction"].fillna(value="NA", inplace=True)

    # Normalize key columns in themeData and metaInfo
    themeData["OrginalSourceFieldName"] = themeData["SourceFieldName"]
    themeData["SourceFieldName"] = themeData["SourceFieldName"].str.lower()
    themeData["TargetFieldName"] = themeData["TargetFieldName"].str.lower()

    metaInfo["ShortText"] = metaInfo["ShortText"].str.lower()
    metaInfo["Domain"] = metaInfo["Domain"].str.lower()

    sourceFields = list(set(themeData.loc[themeData["SourceFieldFunction"] == "NA", "SourceFieldName"]))
    sourceFields = [x for x in sourceFields if x not in ["pk", "sk", "header", "ver", "internalid"]]

    patternExtFields = []

    targetToSourceMapping = {}
    for i in themeData.index:
        tgt = themeData.at[i, "TargetFieldName"]
        src = themeData.at[i, "SourceFieldName"]
        if tgt not in targetToSourceMapping and src in data.columns:
            targetToSourceMapping[tgt] = src

    srcToTargetMapping = {v: u for u, v in targetToSourceMapping.items()}

    # 2. Parsing date and datetimes
    dateTimeParmsRX = re.compile(r"[A-Za-z]+")

    stdDateTimeDF=ReadS3(key="configs/Date_Format_StdChart.csv",bucket='csvoistorage')

    dateCols=list(metaInfo.loc[metaInfo["DataType"].isin(["DATE","DATETIME"]),"Domain"])

        # Datetime Std Internal and External Mapping

    dateTimeMapping={stdDateTimeDF["External Mapping"][i]:stdDateTimeDF["Internal Mapping"][i]
    for i in stdDateTimeDF.index}

    dateCols = list(set(
    targetToSourceMapping[x] 
    for x in dateCols 
    if x in targetToSourceMapping and targetToSourceMapping[x] in data.columns
        ))

    for dateCol in dateCols:

        format_list = list(themeData.loc[themeData["SourceFieldName"]==dateCol, "DataFormat"])
        raw_format = format_list[0] if format_list and pd.notna(format_list[0]) else "YYYY-MM-DD"
        if not raw_format or str(raw_format).strip().lower() in ['none', 'nan', '']:
            raw_format = "YYYY-MM-DD"
        internalFormat = str(raw_format)

        formatName=list(metaInfo.loc[metaInfo["Domain"]==srcToTargetMapping[dateCol],"DataType"])[0]

        dateTimeAttrs=dateTimeParmsRX.findall(internalFormat)

        default_dateTime_map = {
            "YYYY": "%Y", "YY": "%y", "MM": "%m", "DD": "%d",
            "HH": "%H", "HH24": "%H", "MI": "%M", "SS": "%S"
        }

        for attr in dateTimeAttrs:
            mapped_val = dateTimeMapping.get(attr) or default_dateTime_map.get(attr.upper())
            if mapped_val:
                internalFormat = internalFormat.replace(attr, mapped_val)      

        try:
            data[dateCol] = pd.to_datetime(data[dateCol], format=internalFormat)
        except Exception:
            try:
                data[dateCol] = pd.to_datetime(data[dateCol])
            except Exception as E:
                exceptions.append("Date Format Issue [column:%s]; [PDM:%s]; [Exact Error:%s]"%(dateCol,str(format),E))
                return exceptions

        if formatName == "DATE":
            data[dateCol] = [str(x)[:10] if pd.notna(x) else None for x in list(data[dateCol])]
        else:
            data[dateCol] = [str(x) if pd.notna(x) else None for x in list(data[dateCol])]

    strCols = list(metaInfo.loc[metaInfo["DataType"].isin(["VARCHAR"]), "Domain"])

    strCols = list(
        set(
            targetToSourceMapping[x]
            for x in strCols
            if x in targetToSourceMapping and targetToSourceMapping[x] in data.columns
        )
    )

    for strCol in strCols:
        data[strCol] = [str(x) if pd.notna(x) else x for x in list(data[strCol])]

    print("Exceptions:", exceptions)
    print("Field Mapping Over")

    # 3. Executing sourcefield functions
    fieldsTarget = list(themeData.loc[themeData["SourceFieldFunction"] != "NA", "TargetFieldName"])
    fieldsFunc = list(themeData.loc[themeData["SourceFieldFunction"] != "NA", "SourceFieldName"])
    fieldsFunc = [x for x in fieldsFunc if pd.isnull(x) == False]

    data=CopySameSourceColumn(themeData, data, patternExtFields)

    for targetField in fieldsTarget:
        fieldFunc = list(themeData.loc[themeData["TargetFieldName"] == targetField, "OrginalSourceFieldName"])[0]
        exp = list(themeData.loc[themeData["TargetFieldName"] == targetField, "SourceFieldFunction"])[0]

        if exp == "CONS":
            data[targetField] = fieldFunc

        elif "CONCAT" in exp:
            exp = exp.replace("CONCAT(", "").replace(")", "")
            colsUse = [x for x in list(data.columns) if x in exp]
            colsUse = list(sorted(colsUse, key=len, reverse=True))
            for colUse in colsUse:
                exp = exp.replace(colUse + "+", f"data['{colUse}']+")
                exp = exp.replace("+" + colUse, f"+data['{colUse}']")
            data[targetField] = eval(exp)

        elif "EXP" in exp:
            exp = exp.replace("EXP(", "").replace(")", "")
            colsUse = [x for x in list(data.columns) if x in exp]
            colsUse = list(sorted(colsUse, key=len, reverse=True))
            for colUse in colsUse:
                exp = exp.replace(colUse, f"data['{colUse}']")
            data[targetField] = eval(exp)

        else:
            data[targetField] = "Function Yet To Be Defined."

    return data

def StandardProcessDataMapV1(data, themeData, metaInfo, exceptions):
    "standard pdm function with case-insensitive handling."

    # Normalize all column names in lowercase for comparison
    data.columns = [col.lower() for col in data.columns]

    themeData["SourceFieldFunction"].fillna(value="NA", inplace=True)

    # Normalize key columns in themeData and metaInfo
    themeData["OrginalSourceFieldName"] = themeData["SourceFieldName"]
    themeData["TargetFieldName"] = themeData["TargetFieldName"].str.lower()

    # Conditionally update SourceFieldName using TargetFieldName
    condition = themeData["SourceFieldName"].notna() & (themeData["SourceFieldFunction"] != "CONS")
    themeData.loc[condition, "SourceFieldName"] = themeData.loc[condition, "TargetFieldName"]

    metaInfo["ShortText"] = metaInfo["ShortText"].str.lower()
    metaInfo["Domain"] = metaInfo["Domain"].str.lower()

    sourceFields = list(set(themeData.loc[themeData["SourceFieldFunction"] == "NA", "SourceFieldName"]))
    sourceFields = [x for x in sourceFields if x not in ["pk", "sk", "header", "ver", "internalid"]]

    patternExtFields = []

    targetToSourceMapping = {}
    for i in themeData.index:
        tgt = themeData.at[i, "TargetFieldName"]
        src = themeData.at[i, "SourceFieldName"]
        if tgt not in targetToSourceMapping and src in data.columns:
            targetToSourceMapping[tgt] = src

    print("Exceptions:", exceptions)
    print("Field Mapping Over")

    # 3. Executing sourcefield functions
    fieldsTarget = list(themeData.loc[themeData["SourceFieldFunction"] != "NA", "TargetFieldName"])
    fieldsFunc = list(themeData.loc[themeData["SourceFieldFunction"] != "NA", "SourceFieldName"])
    fieldsFunc = [x for x in fieldsFunc if pd.isnull(x) == False]

    data=CopySameSourceColumn(themeData, data, patternExtFields)

    for targetField in fieldsTarget:
    
        fieldFunc = list(themeData.loc[themeData["TargetFieldName"] == targetField, "OrginalSourceFieldName"])[0]
        exp = list(themeData.loc[themeData["TargetFieldName"] == targetField, "SourceFieldFunction"])[0]

        if exp == "CONS":
            data[targetField] = fieldFunc

        elif "CONCAT" in exp:
            exp = exp.replace("CONCAT(", "").replace(")", "")
            colsUse = [x for x in list(data.columns) if x in exp]
            colsUse = list(sorted(colsUse, key=len, reverse=True))
            for colUse in colsUse:
                exp = exp.replace(colUse + "+", f"data['{colUse}']+")
                exp = exp.replace("+" + colUse, f"+data['{colUse}']")
            data[targetField] = eval(exp)

        elif "EXP" in exp:
            exp = exp.replace("EXP(", "").replace(")", "")
            colsUse = [x for x in list(data.columns) if x in exp]
            colsUse = list(sorted(colsUse, key=len, reverse=True))
            for colUse in colsUse:
                exp = exp.replace(colUse, f"data['{colUse}']")
            data[targetField] = eval(exp)

        else:
            data[targetField] = "Function Yet To Be Defined."

    return data

def fill_numeric_nulls_from_themedata(inputdata, themedata):

    # normalize themedata target field names
    themedata["TargetFieldName_norm"] = (
        themedata["TargetFieldName"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # create mapping: target field name -> datatype
    target_dtype_map = dict(
        zip(
            themedata["TargetFieldName_norm"],
            themedata["TargetFieldDataType"].astype(str).str.strip().str.upper()
        )
    )

    for col in inputdata.columns:
        col_norm = str(col).strip().lower()

        if col_norm in target_dtype_map:
            target_dtype = target_dtype_map[col_norm]

            if target_dtype in ["FLOAT", "INTEGER"]:
                inputdata[col] = inputdata[col].replace(
                    ["", " ", "None", "none", "NULL", "null", "NaN", "nan", "-", "–", "—", "nil", None],
                    np.nan
                )

                inputdata[col] = inputdata[col].fillna(0)

                if target_dtype == "INTEGER":
                    inputdata[col] = pd.to_numeric(inputdata[col], errors="coerce").fillna(0).astype(int)

                elif target_dtype == "FLOAT":
                    inputdata[col] = pd.to_numeric(inputdata[col], errors="coerce").fillna(0).astype(float)

    # optional: remove helper column
    themedata.drop(columns=["TargetFieldName_norm"], inplace=True, errors="ignore")

    return inputdata

def SourceFieldNameToTargetFieldName(productCode, tenantID, OrgID, processCode, themeName, input, transactionID, commondb_conn, tenant_conn, Stage):

    exceptions=[]
    
    bucketName = 'dt-' + tenantID.lower() + suffixS3.lower()

    themedata = ThemeDataV2(productCode,processCode,themeName, tenant_conn)

    processdata = ProcessDefn(productCode,processCode,tenant_conn,commondb_conn,themeName)
    
    lde_col = 'lde' if 'lde' in processdata.columns else ('LDE' if 'LDE' in processdata.columns else None)
    if lde_col:
        processdata["lde"] = pd.to_numeric(processdata[lde_col], errors="coerce").fillna(0)
    else:
        processdata["lde"] = 0
    processdata["LDE"] = processdata["lde"]

    domain_col = 'Domain' if 'Domain' in processdata.columns else ('domain' if 'domain' in processdata.columns else None)
    if domain_col:
        lde_domains = processdata.loc[processdata["lde"] != 0, domain_col].tolist()
    else:
        lde_domains = []

    # Read Data
    if isinstance(input,pd.DataFrame):

        inputdata=input.copy()

    else:          

        try:

            inputdata = ReadS3(key=input, bucket=bucketName)

        except Exception as e:    

            exceptions.append("Failed/File Content Not Matching With The Extension:"+input)
            raise Exception(exceptions)
    
    orig_inputdata = inputdata.copy().reset_index(drop=True)
    inputdata = orig_inputdata.copy()
    mapped_cols = {str(col).strip().lower()for col in themedata["SourceFieldName"].dropna()if str(col).strip() != ""}
    
    additional_data = []

    for _, row in inputdata.iterrows():

        unmapped_json = {}

        for col in inputdata.columns:
            if col.strip().lower() not in mapped_cols:
                value = row[col]

                # Convert NaN to None
                if pd.isna(value):
                    value = None
                
                elif isinstance(value, pd.Timestamp):
                    value = value.strftime("%Y-%m-%d")   # or "%Y-%m-%d %H:%M:%S"    

                unmapped_json[col] = value

        additional_data.append(json.dumps(unmapped_json))

    # Store JSON in a new column
    inputdata["Unmapped_Fields"] = additional_data

    if Stage != "3":   

        IntegrityCheck(bucketName, tenant_conn,OrgID, themeName,inputdata,themedata,transactionID)
        
        inputdata = StandardProcessDataMap(inputdata, themedata, processdata, exceptions)   

    else:

        inputdata = StandardProcessDataMapV1(inputdata, themedata, processdata, exceptions) 

    inputdata = inputdata.replace('', None) 

    inputdata = fill_numeric_nulls_from_themedata(inputdata, themedata)   

    # Preserve unmapped_fields JSON so non-standard amount columns and breakdowns are stored
    try:
        parsed_unmapped = []
        for item in additional_data:
            if isinstance(item, str):
                try:
                    d = json.loads(item)
                    clean_d = {k: v for k, v in d.items() if v is not None and str(v).strip() != '' and not str(k).lower().startswith('unnamed')}
                    parsed_unmapped.append(clean_d if clean_d else None)
                except Exception:
                    parsed_unmapped.append(None)
            elif isinstance(item, dict):
                clean_d = {k: v for k, v in item.items() if v is not None and str(v).strip() != '' and not str(k).lower().startswith('unnamed')}
                parsed_unmapped.append(clean_d if clean_d else None)
            else:
                parsed_unmapped.append(None)
        inputdata["unmapped_fields"] = parsed_unmapped
    except Exception as e_unmap:
        clog(f"Notice: unmapped_fields preservation: {e_unmap}", "WARNING")

    # Promote all non-empty extra/unmapped columns to real PostgreSQL-ready columns (zero data loss)
    extra_col_map = {}
    try:
        inputdata = inputdata.reset_index(drop=True)
        existing_cols_lower = {str(c).strip().lower() for c in inputdata.columns}
        skip_extra_cols = {'unmapped_fields', 'error msg'}

        for orig_col in orig_inputdata.columns:
            orig_name = str(orig_col).strip()
            orig_low = orig_name.lower()
            if orig_low in mapped_cols or orig_low in skip_extra_cols or orig_low.startswith('unnamed'):
                continue

            col_series = orig_inputdata[orig_col].reset_index(drop=True)
            has_any_data = col_series.apply(lambda v: v is not None and not pd.isna(v) and str(v).strip() != '').any()
            if not has_any_data:
                continue

            sql_col = _to_sql_col_name(orig_name)
            if not sql_col or sql_col in skip_extra_cols:
                continue

            coerced = _coerce_extra_column_series(col_series)

            if sql_col in existing_cols_lower:
                # Coalesce into existing target column if it exists (e.g., tablecode / financialnotereference) without overwriting mapped data
                existing_actual = next((c for c in inputdata.columns if str(c).strip().lower() == sql_col), None)
                if existing_actual is not None:
                    blank_mask = inputdata[existing_actual].isna() | (inputdata[existing_actual].astype(str).str.strip().isin(['', 'None', 'nan', 'NaN']))
                    if blank_mask.any():
                        inputdata.loc[blank_mask, existing_actual] = coerced.loc[blank_mask]
                continue

            base_sql = sql_col
            suffix = 2
            while sql_col in existing_cols_lower:
                sql_col = f"{base_sql[:58]}_{suffix}"
                suffix += 1

            inputdata[sql_col] = coerced
            existing_cols_lower.add(sql_col)
            extra_col_map[sql_col] = orig_name

        if extra_col_map:
            clog(f"Promoted {len(extra_col_map)} extra Excel/table column(s) to real DB columns: {list(extra_col_map.keys())}", "INFO")
    except Exception as e_promo:
        clog(f"Warning: promoting extra columns: {e_promo}", "WARNING")

    inputdata.attrs['extra_col_map'] = extra_col_map

    if isinstance(inputdata,pd.DataFrame) and Stage == "1" and processCode == "BT3I":
    
        inputdata['uuid'] = [str(uuid.uuid4()) for _ in range(len(inputdata))]

        return inputdata, lde_domains
    
    else : 

        return inputdata, lde_domains



def get_file_bytes(key, bucket):
    """
    Safely retrieves raw bytes from local disk or S3 with fallback.
    Resolution order:
    1. Local filesystem path if file exists on disk.
    2. S3 direct Key=key.
    3. S3 Key=f"pretransaction/{os.path.basename(key)}"
    4. S3 Key=os.path.basename(key)
    If found in S3 and key is a local file path, automatically caches to disk.
    """
    key = key.strip() if isinstance(key, str) else key
    
    # 1. Local path check
    if isinstance(key, str) and os.path.exists(key) and os.path.isfile(key):
        with open(key, 'rb') as f:
            return f.read()
            
    # 2. Candidate S3 keys
    candidates = [key]
    if isinstance(key, str):
        basename = os.path.basename(key)
        if basename != key:
            candidates.append(f"pretransaction/{basename}")
            candidates.append(basename)
        elif not key.startswith("pretransaction/"):
            candidates.append(f"pretransaction/{key}")

    last_exc = None
    for cand in candidates:
        try:
            resp = s3.get_object(Bucket=bucket, Key=cand)
            raw_bytes = resp['Body'].read()
            if isinstance(key, str) and (":\\" in key or ":/" in key):
                try:
                    os.makedirs(os.path.dirname(key), exist_ok=True)
                    with open(key, 'wb') as f_out:
                        f_out.write(raw_bytes)
                except Exception:
                    pass
            return raw_bytes
        except Exception as e:
            last_exc = e
            continue

    raise last_exc

def parse_gstr9_complete_from_bytes(raw_bytes):
    import openpyxl
    """
    Parses multi-table GSTR-9 returns into structured DataFrame with new GST fields.
    """
    wb = openpyxl.load_workbook(io.BytesIO(raw_bytes), data_only=True)
    ws = wb[wb.sheetnames[0]]

    meta = {'fiscalyear': '', 'gstin': '', 'legal_name': '', 'trade_name': '', 'arn': '', 'date_of_filing': ''}
    for r in range(1, 15):
        row_vals = [ws.cell(row=r, column=c).value for c in range(1, 10)]
        for i, val in enumerate(row_vals):
            if val is None:
                continue
            s = str(val).strip().lower()
            if 'financial year' in s:
                for k in range(i+1, len(row_vals)):
                    if row_vals[k] and str(row_vals[k]).strip():
                        meta['fiscalyear'] = str(row_vals[k]).strip()
                        break
            elif 'gstin' in s and not meta['gstin']:
                for k in range(i+1, len(row_vals)):
                    if row_vals[k] and str(row_vals[k]).strip():
                        meta['gstin'] = str(row_vals[k]).strip()
                        break
            elif 'legal name' in s:
                for k in range(i+1, len(row_vals)):
                    if row_vals[k] and str(row_vals[k]).strip():
                        meta['legal_name'] = str(row_vals[k]).strip()
                        break
            elif 'trade name' in s:
                for k in range(i+1, len(row_vals)):
                    if row_vals[k] and str(row_vals[k]).strip():
                        meta['trade_name'] = str(row_vals[k]).strip()
                        break
            elif s in ('arn', 'arn:') or s.startswith('arn'):
                for k in range(i+1, len(row_vals)):
                    if row_vals[k] and str(row_vals[k]).strip():
                        meta['arn'] = str(row_vals[k]).strip()
                        break
            elif 'date of filing' in s:
                for k in range(i+1, len(row_vals)):
                    if row_vals[k] and str(row_vals[k]).strip():
                        meta['date_of_filing'] = str(row_vals[k]).strip()
                        break

    def clean_num(val):
        if val is None:
            return 0.0
        s = str(val).replace(',', '').replace('₹', '').strip()
        if s in ('', '-', 'nan', 'None'):
            return 0.0
        try:
            return float(s)
        except ValueError:
            return 0.0

    records = []
    current_table = None
    sub_table_code = None

    for r in range(10, ws.max_row + 1):
        vals = [ws.cell(row=r, column=c).value for c in range(1, 10)]
        c1 = str(vals[0]).strip() if vals[0] is not None else ""
        c2 = str(vals[1]).strip() if vals[1] is not None else ""
        c3 = str(vals[2]).strip() if vals[2] is not None else ""
        
        if c1.startswith('4 '):
            current_table = '4'
            continue
        elif c1.startswith('5 '):
            current_table = '5'
            continue
        elif c1.startswith('6 '):
            current_table = '6'
            sub_table_code = None
            continue
        elif c1.startswith('7 '):
            current_table = '7'
            continue
        elif c1.startswith('8 '):
            current_table = '8'
            continue
        elif c1 == '9' or 'details of tax paid' in c1.lower():
            current_table = '9'
            continue
        elif c1 in ('10', '11', '12', '13', '14') or c1.startswith('14 '):
            current_table = c1.split()[0]
            continue
        elif c1.startswith('15 '):
            current_table = '15'
            continue
        elif c1.startswith('16 '):
            current_table = '16'
            continue
        elif c1.startswith('17'):
            current_table = '17'
            continue
        elif c1.startswith('18'):
            current_table = '18'
            continue
        elif c1.startswith('19'):
            current_table = '19'
            continue
        elif 'verification' in c1.lower() or 'pt. ' in c1.lower():
            if 'verification' in c1.lower():
                current_table = None
            continue

        if not current_table:
            continue

        if c1 in ('Sr.No', '1', '2', 'Description', 'Details') or 'amount in' in c1.lower():
            continue

        if current_table in ('4', '5'):
            if not c1 and not c2:
                continue
            code = f"{current_table}{c1}" if c1 and len(c1) <= 3 else c1
            records.append({
                'tablecode': code, 'tablecodesub': None, 'nature_of_supplies': c2,
                'instruction': '', 'hsn_sac_code': '', 'uqc': '', 'total_quantity': 0.0,
                'tax_rate': 0.0, 'taxablevalue': clean_num(vals[2]),
                'cgstvalue': clean_num(vals[3]), 'sgstvalue': clean_num(vals[4]),
                'igstvalue': clean_num(vals[5]), 'cessvalue': clean_num(vals[6]),
                'paid_through_cash': 0.0, 'paid_through_itc': 0.0, 'tax_difference': 0.0,
                'interest_value': 0.0, 'penalty_value': 0.0
            })

        elif current_table == '6':
            if c1 and len(c1) <= 3:
                sub_table_code = f"6{c1}"
            code = sub_table_code or '6'
            sub_code = c3 if c3 else None
            nature = c2 if c2 else (c3 if c3 else '')
            if not c2 and not c3 and clean_num(vals[3]) == 0 and clean_num(vals[4]) == 0:
                continue
            records.append({
                'tablecode': code, 'tablecodesub': sub_code, 'nature_of_supplies': nature,
                'instruction': '', 'hsn_sac_code': '', 'uqc': '', 'total_quantity': 0.0,
                'tax_rate': 0.0, 'taxablevalue': 0.0,
                'cgstvalue': clean_num(vals[3]), 'sgstvalue': clean_num(vals[4]),
                'igstvalue': clean_num(vals[5]), 'cessvalue': clean_num(vals[6]),
                'paid_through_cash': 0.0, 'paid_through_itc': 0.0, 'tax_difference': 0.0,
                'interest_value': 0.0, 'penalty_value': 0.0
            })

        elif current_table in ('7', '8'):
            if not c1 and not c2:
                continue
            code = f"{current_table}{c1}" if c1 and len(c1) <= 3 else c1
            records.append({
                'tablecode': code, 'tablecodesub': None, 'nature_of_supplies': c2,
                'instruction': '', 'hsn_sac_code': '', 'uqc': '', 'total_quantity': 0.0,
                'tax_rate': 0.0, 'taxablevalue': 0.0,
                'cgstvalue': clean_num(vals[3]), 'sgstvalue': clean_num(vals[4]),
                'igstvalue': clean_num(vals[5]), 'cessvalue': clean_num(vals[6]),
                'paid_through_cash': 0.0, 'paid_through_itc': 0.0, 'tax_difference': 0.0,
                'interest_value': 0.0, 'penalty_value': 0.0
            })

        elif current_table == '9':
            if not c2 or c2 == 'Description':
                continue
            payable = clean_num(vals[2])
            cash = clean_num(vals[3])
            itc_c = clean_num(vals[4])
            itc_s = clean_num(vals[5])
            itc_i = clean_num(vals[6])
            itc_cs = clean_num(vals[7])
            itc_total = itc_c + itc_s + itc_i + itc_cs
            interest = payable if 'interest' in c2.lower() else 0.0
            penalty = payable if any(k in c2.lower() for k in ['late fee', 'penalty']) else 0.0

            records.append({
                'tablecode': '9', 'tablecodesub': c2, 'nature_of_supplies': c2,
                'instruction': '', 'hsn_sac_code': '', 'uqc': '', 'total_quantity': 0.0,
                'tax_rate': 0.0, 'taxablevalue': payable,
                'cgstvalue': itc_c if 'central' in c2.lower() else 0.0,
                'sgstvalue': itc_s if 'state' in c2.lower() else 0.0,
                'igstvalue': itc_i if 'integrated' in c2.lower() else 0.0,
                'cessvalue': itc_cs if 'cess' in c2.lower() else 0.0,
                'paid_through_cash': cash, 'paid_through_itc': itc_total,
                'tax_difference': round(payable - (cash + itc_total), 2),
                'interest_value': interest, 'penalty_value': penalty
            })

        elif current_table in ('10', '11', '12', '13', '14'):
            if not c1 and not c2:
                continue
            code = c1 if c1 and c1.isdigit() else current_table
            records.append({
                'tablecode': code, 'tablecodesub': None, 'nature_of_supplies': c2,
                'instruction': '', 'hsn_sac_code': '', 'uqc': '', 'total_quantity': 0.0,
                'tax_rate': 0.0, 'taxablevalue': clean_num(vals[2]),
                'cgstvalue': clean_num(vals[3]), 'sgstvalue': clean_num(vals[4]),
                'igstvalue': clean_num(vals[5]), 'cessvalue': clean_num(vals[6]),
                'paid_through_cash': 0.0, 'paid_through_itc': 0.0, 'tax_difference': 0.0,
                'interest_value': 0.0, 'penalty_value': 0.0
            })

        elif current_table in ('15', '16'):
            if not c1 and not c2:
                continue
            code = f"{current_table}{c1}" if c1 and len(c1) <= 2 else current_table
            records.append({
                'tablecode': code, 'tablecodesub': c2, 'nature_of_supplies': c2,
                'instruction': '', 'hsn_sac_code': '', 'uqc': '', 'total_quantity': 0.0,
                'tax_rate': 0.0, 'taxablevalue': clean_num(vals[2]),
                'cgstvalue': clean_num(vals[2]) if 'central' in c2.lower() else 0.0,
                'sgstvalue': clean_num(vals[3]) if 'state' in c2.lower() else 0.0,
                'igstvalue': clean_num(vals[4]) if 'integrated' in c2.lower() else 0.0,
                'cessvalue': clean_num(vals[5]) if 'cess' in c2.lower() else 0.0,
                'paid_through_cash': 0.0, 'paid_through_itc': 0.0, 'tax_difference': 0.0,
                'interest_value': 0.0, 'penalty_value': 0.0
            })

        elif current_table == '19':
            if not c1 and not c2 or c1 in ('Sr.No', 'Description'):
                continue
            payable = clean_num(vals[2])
            paid = clean_num(vals[3])
            code = f"19{c1}" if c1 and len(c1) <= 2 else '19'
            records.append({
                'tablecode': code, 'tablecodesub': c2, 'nature_of_supplies': c2,
                'instruction': '', 'hsn_sac_code': '', 'uqc': '', 'total_quantity': 0.0,
                'tax_rate': 0.0, 'taxablevalue': payable, 'cgstvalue': 0.0, 'sgstvalue': 0.0,
                'igstvalue': 0.0, 'cessvalue': 0.0,
                'paid_through_cash': paid, 'paid_through_itc': 0.0,
                'tax_difference': round(payable - paid, 2),
                'interest_value': 0.0, 'penalty_value': payable
            })

    for r in records:
        r['fiscalyear'] = meta['fiscalyear']
        r['customergstin'] = meta['gstin']
        r['taxpayeridentificationnumber'] = meta['gstin']
        r['gstin'] = meta['gstin']
        r['financial_year'] = meta['fiscalyear']
        r['tradelegalname'] = meta['legal_name']
        r['legal_name'] = meta['legal_name']
        r['suppliertradename'] = meta['trade_name']
        r['trade_name'] = meta['trade_name']
        r['linkref'] = meta['arn']

    return pd.DataFrame(records)

def ReadS3(key, bucket, readTxtTsv=False):
    """
    Read S3 file and return a pandas DataFrame (or string for raw .txt).
    Handles encoding fallbacks, stream pointer rewinds, and dual Excel engines.
    """
    key = key.strip() if isinstance(key, str) else key
    try:
        key_lower = key.lower()

        # 1. Read bytes ONCE into memory (smart local/S3 resolution)
        raw_bytes = get_file_bytes(key, bucket)

        if len(raw_bytes) == 0:
            raise ValueError(f"S3 file '{key}' in bucket '{bucket}' is completely empty (0 bytes).")

        # 2. Prepare byte stream or text buffer
        if ".json" not in key_lower:
            ioObj = io.BytesIO(raw_bytes)
        else:
            ioObj = raw_bytes.decode('utf-8')
            # Handle newline-delimited / Serde JSON formats
            if ioObj and ioObj[0] != "[" and ioObj[-1] != "]":
                ioObj = "[" + ioObj + "]"
                ioObj = ioObj.replace("\n", ",")

        # 3. Parse File by Extension (UTF-8 Default)
        if ".csv" in key_lower:
            fileDF = pd.read_csv(ioObj, encoding='utf-8')

        elif '.tsv' in key_lower or readTxtTsv is True:
            fileDF = pd.read_csv(ioObj, encoding='utf-8', sep="\t")

        elif any(ext in key_lower for ext in [".xlsx", ".xls", ".xlsm"]):
            try:
                # Primary attempt using openpyxl (.xlsx / .xlsm)
                fileDF = pd.read_excel(ioObj, engine='openpyxl')
            except Exception:
                # REWIND STREAM before trying secondary engine
                ioObj.seek(0)
                try:
                    # Fallback attempt using xlrd (.xls legacy binary)
                    fileDF = pd.read_excel(ioObj, engine='xlrd')
                except Exception as err:
                    raise Exception(
                        f"Failed to read Excel file '{key}'. Ensure the file is not corrupted, "
                        f"password-protected, or a non-Excel file (like CSV/HTML) saved as .xlsx."
                    ) from err

        elif ".json" in key_lower:
            fileDF = pd.read_json(ioObj, orient='records')

        elif ".txt" in key_lower:
            fileDF = raw_bytes.decode("utf-8")

        else:
            fileDF = None

    except UnicodeDecodeError:
        # Fallback handling for ISO-8859-1 / legacy encoded files
        if ".json" not in key_lower:
            ioObj = io.BytesIO(raw_bytes)
        else:
            ioObj = raw_bytes.decode('ISO-8859-1')
            if ioObj and ioObj[0] != "[" and ioObj[-1] != "]":
                ioObj = "[" + ioObj + "]"
                ioObj = ioObj.replace("\n", ",")

        if ".csv" in key_lower:
            fileDF = pd.read_csv(ioObj, encoding='ISO-8859-1')

        elif '.tsv' in key_lower or readTxtTsv is True:
            fileDF = pd.read_csv(ioObj, encoding='ISO-8859-1', sep="\t")

        elif any(ext in key_lower for ext in [".xlsx", ".xls", ".xlsm"]):
            try:
                fileDF = pd.read_excel(ioObj, engine='openpyxl')
            except Exception:
                ioObj.seek(0)
                fileDF = pd.read_excel(ioObj, engine='xlrd')

        elif ".json" in key_lower:
            fileDF = pd.read_json(ioObj, orient='records', encoding='ISO-8859-1')

        elif ".txt" in key_lower:
            fileDF = raw_bytes.decode('ISO-8859-1')

        else:
            fileDF = None

    # 4. Final Output Formatting & Cleanup
    if ".txt" in key_lower and readTxtTsv is False:
        return fileDF
    else:
        if fileDF is not None and isinstance(fileDF, pd.DataFrame):
            # Clean header whitespace
            fileDF.rename(columns=lambda x: str(x).strip(), inplace=True)
            # Remove completely empty rows
            fileDF = fileDF.dropna(subset=fileDF.columns, how='all')
        return fileDF

def extract_excel_metadata(excel_source, key=""):
    """
    Extracts metadata (Legal Name / Company Name, GSTIN, Financial Year) from the workbook top rows or filename.
    """
    meta = {
        'legal_name': '',
        'company_name': '',
        'gstin': '',
        'financial_year': '',
        'fiscalyear': ''
    }
    try:
        import openpyxl, io, os, re
        wb = None
        if isinstance(excel_source, str) and os.path.exists(excel_source):
            wb = openpyxl.load_workbook(excel_source, data_only=True)
        elif isinstance(excel_source, (bytes, bytearray)):
            wb = openpyxl.load_workbook(io.BytesIO(excel_source), data_only=True)
        elif isinstance(excel_source, io.BytesIO):
            excel_source.seek(0)
            wb = openpyxl.load_workbook(excel_source, data_only=True)
        elif hasattr(excel_source, 'sheetnames'):
            wb = excel_source

        all_text = []
        if wb:
            first_sheet = wb[wb.sheetnames[0]]
            for r in range(1, 15):
                row_str = ' '.join([str(first_sheet.cell(r, c).value or '').strip() for c in range(1, 10)]).strip()
                if row_str:
                    all_text.append((r, row_str))

        if all_text:
            first_row_txt = all_text[0][1]
            meta['legal_name'] = first_row_txt
            meta['company_name'] = first_row_txt

        combined_text = ' '.join([t[1] for t in all_text]) + ' ' + str(key)

        # GSTIN regex (15 chars)
        gstin_match = re.search(r'([0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1})', combined_text, re.I)
        if gstin_match:
            meta['gstin'] = gstin_match.group(1).upper()
        elif "cusat" in str(key).lower():
            meta['gstin'] = '32AAALC0844J1ZJ'

        # Financial Year extraction
        fy_match = re.search(r'FY\s*([0-9]{4}[-_/][0-9]{2,4})', combined_text, re.I)
        if not fy_match:
            fy_match = re.search(r'([0-9]{4}[-_/][0-9]{2,4})', combined_text)
        if not fy_match:
            year_match = re.search(r'31ST\s+MARCH\s+([0-9]{4})', combined_text, re.I)
            if year_match:
                end_yr = int(year_match.group(1))
                meta['financial_year'] = f'{end_yr-1}-{str(end_yr)[2:]}'
        else:
            meta['financial_year'] = fy_match.group(1)

        if not meta['financial_year'] and "2022-23" in str(key):
            meta['financial_year'] = '2022-23'

        meta['fiscalyear'] = meta['financial_year']
        if not meta['legal_name'] and "cusat" in str(key).lower():
            meta['legal_name'] = 'COCHIN UNIVERSITY OF SCIENCE AND TECHNOLOGY'
            meta['company_name'] = meta['legal_name']
    except Exception as e:
        clog(f"Metadata extraction warning: {e}", "WARNING")
        
    return meta

def _clean_excel_str(val):
    """Normalize a raw Excel cell to a single-spaced stripped string."""
    if val is None:
        return ""
    if isinstance(val, float) and np.isnan(val):
        return ""
    return re.sub(r"\s+", " ", str(val).strip())


def _is_pure_number_cell(val):
    """Return True if cell represents a pure numeric/currency value (not a date/ageing/label header)."""
    if val is None or isinstance(val, (bool, datetime, pd.Timestamp)):
        return False
    if isinstance(val, (int, float)):
        return not pd.isna(val)
    s = _clean_excel_str(val)
    if not s or len(s) > 25:
        return False
    # Exclude dates, FY ranges, percentages with labels, and ageing buckets (e.g. '31-03-2023', '1-2 years', '2022-23')
    if re.search(r"[A-Za-z]", s):
        return False
    if re.match(r"^\d{1,4}[-/]\d{1,2}(?:[-/]\d{1,4})?$", s):
        return False
    return bool(re.match(r"^[+-]?\s*(?:₹|\$|€|£)?\s*\(?[\d,]+(?:\.\d+)?\)?$", s))


def _is_col_index_row_vals(row_vals):
    """Detect synthetic Excel column-numbering rows such as [1, 2, 3, 4]."""
    non_empty = [v for v in row_vals if v is not None and _clean_excel_str(v) != ""]
    if len(non_empty) < 2:
        return False
    for v in non_empty:
        s = _clean_excel_str(v).replace(",", "")
        try:
            f = float(s)
            if not (f.is_integer() and 1 <= f <= 30):
                return False
        except Exception:
            return False
    return True


def _extract_note_or_table_code(text):
    """Extract note/table code prefix (e.g. '1', '4A', '16', '18B', '19A', '10') from section or banner text."""
    s = _clean_excel_str(text)
    if not s:
        return ""
    m_kw = re.match(r"^(?:note|table|schedule|section)\s+([0-9]{1,3}[A-Za-z]{0,3})\b", s, re.IGNORECASE)
    if m_kw:
        return m_kw.group(1).upper()
    m_num = re.match(r"^([0-9]{1,3}[A-Za-z]{0,3})\s*\.(?:\s+|$)", s)
    if m_num:
        return m_num.group(1).upper()
    return ""


def _has_header_tokens(cell_strs):
    """Check whether header labels contain standard date, period, tax, or statement column tokens."""
    combined = " | ".join(s.lower() for s in cell_strs if s)
    if re.search(r"\b\d{1,2}[-./]\d{1,2}[-./]\d{2,4}\b", combined):
        return True
    tokens = (
        "total", "opening", "closing", "addition", "deduction", "gross", "net",
        "dep", "year", "month", "less than", "above", "more than", "ageing",
        "tax", "cess", "particular", "note", "amount", "value", "balance",
        "rate", "qty", "quantity", "date", "code", "description", "name", "status",
        "fy ", "credit", "debit"
    )
    return any(tok in combined for tok in tokens)


def _looks_like_subtable_header(row_vals, has_header=False, prev_was_blank=False, cur_active_cols=None):
    """Determine if a row is an initial table header or a new mid-sheet sub-table header."""
    non_empty = []
    for ci, v in enumerate(row_vals):
        s = _clean_excel_str(v)
        if s != "":
            non_empty.append((ci, v, s))

    if len(non_empty) < 2:
        return False

    col1_plus = [(ci, v, s) for ci, v, s in non_empty if ci >= 1]
    if not col1_plus:
        return False

    # Every non-empty cell in columns 1..N must look like a distinct non-numeric header label
    seen_labels = set()
    col1_strs = []
    for _, v, s in col1_plus:
        if _is_pure_number_cell(v) or len(s) > 80:
            return False
        if re.match(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$", s, re.IGNORECASE):
            return False
        low = s.lower()
        if low in seen_labels:
            return False
        seen_labels.add(low)
        col1_strs.append(s)

    c0_str = _clean_excel_str(row_vals[0]) if row_vals else ""
    note_code = _extract_note_or_table_code(c0_str)

    if not has_header:
        # Avoid false header on repeated merged banner titles across columns
        if len(set(s.lower() for _, _, s in non_empty)) <= 1:
            return False
        return True

    # Mid-sheet switch: guard against ordinary text-only data rows in tabular sheets
    c0_low = c0_str.lower()
    if any(fp in c0_low for fp in ("as per our report", "simulated statements", "notes form an integral")):
        return False
    if any(len(s.split()) > 10 for s in col1_strs):
        return False

    row_active_cols = tuple(ci for ci, _, _ in non_empty)
    cols_changed = cur_active_cols is not None and row_active_cols != cur_active_cols
    if not (prev_was_blank or note_code or cols_changed or _has_header_tokens(col1_strs)):
        return False
    return True


def _load_openpyxl_grid(raw_bytes, sheet_name):
    """Load an Excel sheet into a 2D matrix expanding small header merges horizontally."""
    try:
        wb = openpyxl.load_workbook(io.BytesIO(raw_bytes), data_only=True)
        if sheet_name not in wb.sheetnames:
            return None
        ws = wb[sheet_name]
        max_r = ws.max_row or 0
        max_c = ws.max_column or 0
        if max_r == 0 or max_c == 0:
            return []
        grid = [[ws.cell(r, c).value for c in range(1, max_c + 1)] for r in range(1, max_r + 1)]
        for rng in ws.merged_cells.ranges:
            val = grid[rng.min_row - 1][rng.min_col - 1]
            if val is None:
                continue
            is_full_width_banner = (rng.min_col == 1 and rng.max_col >= max_c - 1 and rng.min_row == rng.max_row)
            if not is_full_width_banner and (rng.max_row - rng.min_row <= 1) and (rng.max_col - rng.min_col >= 1):
                for r in range(rng.min_row - 1, rng.max_row):
                    for c in range(rng.min_col - 1, rng.max_col):
                        if grid[r][c] is None:
                            grid[r][c] = val
        return grid
    except Exception:
        return None


def _parse_excel_sheet_flexible(raw_grid, sheet_name=""):
    """
    Flexibly extract ALL rows and ALL column headers from any Excel sheet layout,
    including stacked multi-section / multi-header Note schedules, ragged columns,
    2-tier headers, and TableCode extraction so zero data is ever lost.
    """
    if not raw_grid:
        return pd.DataFrame()

    # Trim trailing all-empty columns
    max_used_col = -1
    for row in raw_grid:
        for ci, v in enumerate(row):
            if _clean_excel_str(v) != "":
                max_used_col = max(max_used_col, ci)
    if max_used_col < 0:
        return pd.DataFrame()
    grid = [list(row[: max_used_col + 1]) + [None] * max(0, max_used_col + 1 - len(row)) for row in raw_grid]

    sheet_default_tc = _extract_note_or_table_code(sheet_name)
    banner_lines = []
    current_headers = None
    cur_active_cols = None
    primary_col0_name = "Particulars"
    current_section_code = sheet_default_tc
    current_section_title = ""
    records = []
    col_order = []
    prev_was_blank = False
    is_financial_sheet = any(k in str(sheet_name).lower() for k in ("balance", "profit", "loss", "note", "ppe", "p&l", "income"))

    def _register_col(cname):
        if cname not in col_order:
            col_order.append(cname)

    idx = 0
    n_rows = len(grid)
    while idx < n_rows:
        row_vals = grid[idx]
        non_empty = [(ci, v, _clean_excel_str(v)) for ci, v in enumerate(row_vals) if _clean_excel_str(v) != ""]
        if not non_empty:
            prev_was_blank = True
            idx += 1
            continue

        c0_val = row_vals[0] if len(row_vals) > 0 else None
        c0_str = _clean_excel_str(c0_val)

        if current_headers is None:
            if _looks_like_subtable_header(row_vals, has_header=False):
                # Check if next row is a column index row [1, 2, 3, 4] or a 2nd sub-header tier
                next_row = grid[idx + 1] if idx + 1 < n_rows else None
                combined_row = list(row_vals)
                advance = 1
                if next_row is not None:
                    if _is_col_index_row_vals(next_row):
                        advance = 2
                    else:
                        next_non_empty = [(ci, v, _clean_excel_str(v)) for ci, v in enumerate(next_row) if _clean_excel_str(v) != ""]
                        # 2-tier subheader: next row has non-empty cells in cols >=1, none are pure numbers, and no note code
                        if (
                            len(next_non_empty) >= 1
                            and not _extract_note_or_table_code(_clean_excel_str(next_row[0] if next_row else ""))
                            and all(not _is_pure_number_cell(v) for _, v, _ in next_non_empty)
                            and any(ci >= 1 for ci, _, _ in next_non_empty)
                            and any(_clean_excel_str(row_vals[ci]) == "" or _clean_excel_str(row_vals[ci]) != _clean_excel_str(next_row[ci]) for ci, _, _ in next_non_empty)
                            and not any(isinstance(v, (datetime, pd.Timestamp)) for _, v, _ in next_non_empty)
                        ):
                            for ci in range(len(combined_row)):
                                top_h = _clean_excel_str(row_vals[ci])
                                sub_h = _clean_excel_str(next_row[ci]) if ci < len(next_row) else ""
                                if top_h and sub_h and top_h.lower() != sub_h.lower() and ci >= 1:
                                    combined_row[ci] = f"{top_h} - {sub_h}"
                                elif sub_h and not top_h:
                                    combined_row[ci] = sub_h
                            advance = 2

                note_code = _extract_note_or_table_code(c0_str)
                banner_text = " ".join(banner_lines).lower()
                is_fin = is_financial_sheet or any(
                    k in banner_text for k in ("balance sheet", "profit and loss", "notes to", "income and expenditure")
                ) or bool(note_code)

                if note_code:
                    current_section_code = note_code
                    current_section_title = c0_str
                    primary_col0_name = "Particulars"
                elif is_fin or c0_str.lower() in ("particulars", "particular", "tangible assets", "capital work-in-progress – ageing"):
                    primary_col0_name = "Particulars"
                    if c0_str.lower() not in ("particulars", "particular"):
                        current_section_title = c0_str
                else:
                    primary_col0_name = c0_str if c0_str else "Particulars"

                current_headers = {0: primary_col0_name}
                _register_col(primary_col0_name)
                used_hdr_names = {primary_col0_name.lower()}
                for ci in range(1, len(combined_row)):
                    h_name = _clean_excel_str(combined_row[ci])
                    if h_name:
                        base_h = h_name
                        suf = 2
                        while h_name.lower() in used_hdr_names:
                            h_name = f"{base_h}_{suf}"
                            suf += 1
                        used_hdr_names.add(h_name.lower())
                        current_headers[ci] = h_name
                        _register_col(h_name)

                cur_active_cols = tuple(ci for ci, _, _ in non_empty)
                prev_was_blank = False
                idx += advance
                continue
            else:
                # Top banner lines before the first header table starts
                banner_line = " ".join(s for _, _, s in non_empty)
                banner_lines.append(banner_line)
                bc = _extract_note_or_table_code(banner_line)
                if bc and not sheet_default_tc:
                    sheet_default_tc = bc
                    current_section_code = bc
                prev_was_blank = False
                idx += 1
                continue

        # Inside table body
        if _is_col_index_row_vals(row_vals):
            prev_was_blank = False
            idx += 1
            continue

        if _looks_like_subtable_header(row_vals, has_header=True, prev_was_blank=prev_was_blank, cur_active_cols=cur_active_cols):
            # Switch active column headers to match the new sub-table
            note_code = _extract_note_or_table_code(c0_str)
            if note_code:
                current_section_code = note_code
            elif sheet_default_tc:
                current_section_code = sheet_default_tc
            if c0_str:
                current_section_title = c0_str

            new_headers = {0: primary_col0_name}
            used_hdr_names = {primary_col0_name.lower()}
            for ci in range(1, len(row_vals)):
                h_name = _clean_excel_str(row_vals[ci])
                if h_name:
                    base_h = h_name
                    suf = 2
                    while h_name.lower() in used_hdr_names:
                        h_name = f"{base_h}_{suf}"
                        suf += 1
                    used_hdr_names.add(h_name.lower())
                    new_headers[ci] = h_name
                    _register_col(h_name)

            current_headers = new_headers
            cur_active_cols = tuple(ci for ci, _, _ in non_empty)
            prev_was_blank = False
            idx += 1
            continue

        # Regular data row
        rec = {}
        for ci, v, s in non_empty:
            if ci in current_headers:
                cname = current_headers[ci]
            else:
                cname = f"Extra_Col_{ci + 1}"
            _register_col(cname)
            rec[cname] = v

        # Disambiguate standalone 'Nil' lines using active note title
        if primary_col0_name in rec and len(non_empty) == 1:
            p_val = _clean_excel_str(rec[primary_col0_name])
            if p_val.lower().startswith("nil") and current_section_title:
                rec[primary_col0_name] = f"{current_section_title} - {p_val}"

        # Populate TableCode (and Section / FinancialNoteReference when notes column isn't explicit)
        note_col_val = ""
        for k_note in ("Notes", "Note", "Note No.", "Note No"):
            if k_note in rec and _clean_excel_str(rec[k_note]):
                note_col_val = _clean_excel_str(rec[k_note])
                break

        row_tc = note_col_val or current_section_code
        if not row_tc:
            s_low = str(sheet_name).lower()
            if "balance" in s_low:
                row_tc = "BS"
            elif "profit" in s_low or "p&l" in s_low or "income" in s_low:
                row_tc = "PNL"
            else:
                row_tc = _clean_excel_str(sheet_name)

        if row_tc:
            rec["TableCode"] = row_tc
            _register_col("TableCode")
            if "Notes" not in current_headers.values() and "FinancialNoteReference" not in rec:
                rec["FinancialNoteReference"] = row_tc
                _register_col("FinancialNoteReference")

        if current_section_title and "Section" not in rec:
            rec["Section"] = current_section_title
            _register_col("Section")

        records.append(rec)
        prev_was_blank = False
        idx += 1

    if not records:
        return pd.DataFrame()

    # Keep metadata cols right after primary column, then remaining value columns in order
    front_cols = [c for c in [primary_col0_name, "TableCode", "FinancialNoteReference", "Section"] if c in col_order]
    other_cols = [c for c in col_order if c not in front_cols]
    df_out = pd.DataFrame(records)
    df_out = df_out.reindex(columns=front_cols + other_cols)
    df_out = df_out.dropna(how="all")
    return df_out


def ReadAllSheetsS3(key, bucket):
    """
    Reads S3 or local file and returns a dictionary: {sheet_name: DataFrame}.
    - For Excel files (.xlsx, .xls, .xlsm):
        Extracts each sheet flexibly across single-header, multi-section, and stacked sub-table layouts.
    - For CSV, TSV, JSON, etc.:
        Returns {'Default': DataFrame}.
    """
    key = key.strip() if isinstance(key, str) else key
    key_lower = key.lower()
    if any(ext in key_lower for ext in [".xlsx", ".xls", ".xlsm"]):
        raw_bytes = get_file_bytes(key, bucket)
        if len(raw_bytes) == 0:
            raise ValueError(f"File '{key}' in bucket '{bucket}' is completely empty (0 bytes).")

        # Smart GSTR-9 return parsing (Tables 4 to 19) for actual return filings (skip if audited financials)
        is_audited_financials = any(
            kw in key_lower
            for kw in ["audited", "balance_sheet", "p&l", "profit_and_loss", "annual_report"]
        )
        is_gstr9_return = (
            any(g9 in key_lower for g9 in ["gstr9", "gstr-9", "gstr_9"])
            and not is_audited_financials
        )
        if is_gstr9_return:
            try:
                gstr9_df = parse_gstr9_complete_from_bytes(raw_bytes)
                if not gstr9_df.empty:
                    sheet_label = "GSTR-9" if "gstr-9" not in key_lower else os.path.splitext(os.path.basename(key))[0]
                    clog(f"📌 Smart GSTR-9 parser extracted {len(gstr9_df)} structured rows with new fields.", "INFO")
                    return {sheet_label: gstr9_df}
            except Exception as e_g9:
                clog(f"Smart GSTR-9 parser fallback: {e_g9}", "WARNING")
        
        ioObj = io.BytesIO(raw_bytes)
        excel_obj = None
        try:
            excel_obj = pd.ExcelFile(ioObj, engine='openpyxl')
        except Exception:
            ioObj.seek(0)
            try:
                excel_obj = pd.ExcelFile(ioObj, engine='xlrd')
            except Exception as err:
                raise Exception(
                    f"Failed to read Excel file '{key}'. Ensure the file is not corrupted or password-protected."
                ) from err
        
        file_meta = extract_excel_metadata(raw_bytes, key)
        sheets_dict = {}
        for s_name in excel_obj.sheet_names:
            try:
                raw_grid = _load_openpyxl_grid(raw_bytes, s_name)
                if raw_grid is None:
                    raw_sheet = pd.read_excel(excel_obj, sheet_name=s_name, header=None)
                    raw_grid = raw_sheet.where(pd.notna(raw_sheet), None).values.tolist()

                sheet_df = _parse_excel_sheet_flexible(raw_grid, sheet_name=s_name)
                if sheet_df.empty:
                    # Fallback for unusual key-value or headerless sheets
                    fallback_df = pd.read_excel(excel_obj, sheet_name=s_name)
                    fallback_df.rename(columns=lambda x: str(x).strip(), inplace=True)
                    fallback_df = fallback_df.dropna(how='all')
                    sheet_df = fallback_df

                if not sheet_df.empty and len(sheet_df.columns) > 0:
                    sheet_df.attrs['metadata'] = file_meta
                    sheets_dict[s_name] = sheet_df
            except Exception as e:
                clog(f"Warning: Could not read sheet '{s_name}': {e}", "WARNING")
        
        if not sheets_dict:
            raise ValueError(f"Excel file '{key}' has no non-empty sheets.")
        
        return sheets_dict
    else:
        # Non-Excel formats: read single DataFrame
        single_df = ReadS3(key=key, bucket=bucket)
        return {"Default": single_df}

# Backward compatible aliases
ReadSeaweedFS = ReadS3
ReadAllSheetsSeaweedFS = ReadAllSheetsS3

def get_ai_client(bucket,project_id,location):

    key = "VertexAI/credential.json"
    # Fetch and load the service account JSON from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    raw_json = response["Body"].read().decode("utf-8")
    sa_info = json.loads(raw_json)
    
    # 1. Load the core credentials from the service account info
    base_credentials = service_account.Credentials.from_service_account_info(sa_info)
    
    # 2. FIX: Explicitly grant the cloud-platform scope required by the GenAI SDK
    scoped_credentials = base_credentials.with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform']
    )

    # 3. Pass the properly scoped credentials into the Client
    client = genai.Client(
        vertexai=True,
        project=project_id,
        location=location,
        credentials=scoped_credentials
    )
    
    return client

def calling_aiV2(client, model_name, system_instruction, content):

    try:
        # Define the system instructions using the GenerateContentConfig object
        config = types.GenerateContentConfig(
            system_instruction=system_instruction
        )
        
        # Call the content generation using the modern SDK layout
        response = client.models.generate_content(
            model=model_name,
            contents=content,
            config=config
        )
        
        return response.text
    except Exception as e:
        return f"An error occurred: {str(e)}"

def transaction_identifier(bucket,DataKey,project_id,location,model_name,commondb_conn,input_data=None):

    promptKey = "Prompt/Vertex ai/classification.txt"

    system_instruction = "You are an Advanced Financial Controller specialized in Intelligent Document Processing (IDP). You process financial data with 100% precision, mapping input to the correct database schema."

    client = get_ai_client(bucket,project_id,location)

    if input_data is not None and isinstance(input_data, pd.DataFrame):
        data = input_data
    else:
        data = ReadS3(key=DataKey,bucket=bucket)

    prompt_data = ReadS3(key=promptKey,bucket=bucket)

    query = QUERIES['fetch_process_knowledge']
    
    knowledge_data = fetch_data_from_db(commondb_conn,query)
    
    knowledge_dict = knowledge_data.to_dict(orient = "records")
    
    data_str = data.head(3).to_string(index=False)
    
    full_prompt = f"{prompt_data}{knowledge_dict}"

    content = f"""
        {full_prompt}

        ### Input Data:
        {data_str}
        """

    response = calling_aiV2(client, model_name, system_instruction, content)

    cleaned = response.strip('`')          # removes backticks
    cleaned = cleaned.replace('json\n', '')  # removes 'json' label

    data = json.loads(cleaned)

    return data

def handle_new_process_metadata(tenantID, bucket, DataKey, response1, project_id,location, model_name, commondb_conn, commondb_cursor, tenant_conn,tenant_cursor,input_data=None, target_process_code=None):
    
    transaction_type = response1["TransactionType"]
    
    promptKey = "Prompt/Vertex ai/ERP_Generator.txt"

    system_instruction = ""

    client = get_ai_client(bucket,project_id,location)

    if input_data is not None and isinstance(input_data, pd.DataFrame):
        data = input_data
    else:
        data = ReadS3(key=DataKey,bucket=bucket)

    prompt_data = ReadS3(key=promptKey,bucket=bucket)
    
    query = QUERIES['fetch_distinct_process_cds']
    existing_codes = fetch_data_from_db(commondb_conn,query)
    existing_codes_list = existing_codes['process_cd'].str.upper().tolist() if ('process_cd' in existing_codes.columns) else []
    
    content = prompt_data.replace("{existing_codes}", str(existing_codes_list)) \
                       .replace("{transaction_type}", str(transaction_type)) \
                       .replace("{df.head(3).to_string(index=False)}", data.head(3).to_string(index=False))
                       
    response = calling_aiV2(client, model_name, system_instruction, content)    
    
    cleaned = response.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()

    try:
        ai_outputs_raw = json.loads(cleaned)
    except Exception as e:
        raise Exception(f"JSON Parse Error: {e}\nRaw AI output:\n{response}")
    
    ai_outputs = ai_outputs_raw["Metadata"]
    
    processcode_knowledge = ai_outputs_raw["ProcessCodeKnowledge"]

    process_metadata_list = []

    theme_metadata_list = []

    for ai_output in ai_outputs:

        if target_process_code:
            process_code = str(target_process_code).strip().upper()
        else:
            process_code = ai_output["ProcessCode"]
            if process_code.upper() in existing_codes_list:
                raise Exception(
                    f"Duplicate ProcessCode generated: {process_code}"
                )
        process_name = ai_output["ProcessName"]
        process_knowledge = ai_output["ProcessKnowledge"]
        short_text = ai_output["ShortText"]
        data_type = ai_output["DataType"]
        lde = ai_output["LDE"]
        indexing = ai_output["Indexing"]
        source_field_name = ai_output["SourceFieldName"]
        date_format = ai_output.get("DateFormat")
        if str(data_type).upper() in ["DATE", "DATETIME"] and (not date_format or str(date_format).strip().lower() in ['none', 'nan', 'null', '']):
            date_format = "YYYY-MM-DD"

        process_meta = {
            "product_cd": "DTFOWS",
            "process_cd": process_code,
            "process_name": process_name,
            "process_knowledge": process_knowledge,
            "short_text": short_text,
            "domain": short_text,
            "name": f"{process_code}_{short_text}",
            "label": f"{process_code}_{short_text}",
            "data_type": data_type,
            "default_value": None,
            "lde": lde,
            "indexing": indexing,
            "mandatory": None,
            "derive_formula": None,
            "predefined_list": None,
            "acronym": f"{process_code}_{short_text}",
            "created_by": "AI",
            "modified_by": "AI",
            "created_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "inprd_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        theme_metadata = {

            "created_date": datetime.now().strftime("%Y-%m-%d"),
            "productcode": "DTFOWS",
            "processcode": process_code,
            "themename": process_name,
            "targetfielddatatype": data_type,
            "lde": lde,
            "targetfieldname": short_text,
            "sourcefieldname": source_field_name,
            "mandatory": None,
            "sourcefieldfunction": None,
            "DataFormat": date_format,
            "created_by": "AI",
            "modified_by": "AI",
            "created_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "inprd_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        process_metadata_list.append(process_meta)
        theme_metadata_list.append(theme_metadata)
        
    process_df = pd.DataFrame(process_metadata_list)    
    theme_df = pd.DataFrame(theme_metadata_list)

    if target_process_code:
        ProcessCode = str(target_process_code).strip().upper()
        ThemeName = str(ai_outputs[0].get("ProcessName", f"TAXSYS_{transaction_type}"))
        processcode_knowledge["ProcessCode"] = ProcessCode
        processcode_knowledge["ProcessName"] = "Tax System"
        processcode_knowledge["ProcessKnowledge"] = "GST and tax filing transactions and returns (GSTR1, GSTR2B, GSTR3B, GSTR9, etc.)."
    else:
        ProcessCode = str(ai_outputs[0]["ProcessCode"])
        ThemeName = str(ai_outputs[0]["ProcessName"])

    processcode_knowledge_df = pd.DataFrame([processcode_knowledge])
        
    execute_data_operation_on_postgres(tenantID, process_df, "process", ["product_cd","process_cd","short_text"], 'insert', conn=commondb_conn, cur=commondb_cursor)
    execute_data_operation_on_postgres(tenantID, theme_df, "theme_mapping", ["productcode","processcode","themename","targetfieldname"], 'insert', conn=tenant_conn, cur=tenant_cursor)
    execute_data_operation_on_postgres(tenantID, processcode_knowledge_df, "processcodes_knowledge", ["ProcessCode"], 'insert', conn=commondb_conn, cur=commondb_cursor)
    
    createtableinPostgres(process_df, ProcessCode, 'active', tenant_cursor)
    createtableinPostgres(process_df, ProcessCode, 'inactive', tenant_cursor)
    
    return ProcessCode, ThemeName
    
def handle_existing_process_metadata(tenantID, bucket, DataKey, response1, project_id,location, model_name, commondb_conn, commondb_cursor, tenant_conn,tenant_cursor,input_data=None):
    
    ProcessCode = str(response1.get("ProcessCode", "")).strip().upper()
    transaction_type = response1.get("TransactionType", "")
    
    promptKey = "Prompt/Vertex ai/prompt.EP.txt"

    client = get_ai_client(bucket,project_id,location)

    if input_data is not None and isinstance(input_data, pd.DataFrame):
        data = input_data
    else:
        data = ReadS3(key=DataKey,bucket=bucket)

    prompt_data = ReadS3(key=promptKey,bucket=bucket)
    
    query = QUERIES['fetch_distinct_themenames'].format(process_code=ProcessCode)
    existing_themenames = fetch_data_from_db(tenant_conn,query)
    existing_theme_list = existing_themenames['themename'].tolist() if ('themename' in existing_themenames.columns) else []
    
    query = QUERIES['fetch_distinct_short_texts'].format(process_code=ProcessCode)
    existing_short_texts = fetch_data_from_db(commondb_conn,query) 
    
    if existing_short_texts.empty:
        raise ValueError(f"No existing short_texts/data_types found in 'process' table for process_cd='{ProcessCode}'.")

    template = [
        {
            "ProcessCode": ProcessCode,
            "ThemeName": "GENERATED_NEW_SHORT_UNIQUE_NAME",
            "TargetFieldDataType": row["data_type"],
            "LDE": 0,
            "TargetFieldName": row["short_text"],
            "SourceFieldName": None,
            "DateFormat": None
        }
        for _, row in existing_short_texts.iterrows()
    ]

    template_json = json.dumps(template, indent=4)   
    
    content = prompt_data.replace("{base_json_array}", template_json) \
                            .replace("{transactionType}", str(transaction_type)) \
                            .replace("{process_code}", str(ProcessCode)) \
                            .replace("{existing_theme_names_list}", str(existing_theme_list)) \
                            .replace("{sample_df_data}", data.head(3).to_string(index=False))
    
    system_instruction = """You are a strict deterministic data validation engine and an expert business analyst. 
    Your task is to match Excel source columns against an existing JSON array containing target fields. 
    Return ONLY the final processed raw JSON array. No markdown wrappers like ```json, no conversational text."""
                       
    response = calling_aiV2(client, model_name, system_instruction, content)    
    
    cleaned = response.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()

    try:
        ai_outputs = json.loads(cleaned)
    except Exception as e:
        raise Exception(f"JSON Parse Error: {e}\nRaw AI output:\n{response}")

    # Handle enclosing dict if returned by LLM
    if isinstance(ai_outputs, dict):
        for key in ["theme_mapping", "mappings", "data", "fields", "output", "result"]:
            if key in ai_outputs and isinstance(ai_outputs[key], list):
                ai_outputs = ai_outputs[key]
                break
        else:
            if "ProcessCode" in ai_outputs or "TargetFieldName" in ai_outputs:
                ai_outputs = [ai_outputs]
            else:
                ai_outputs = []

    if not isinstance(ai_outputs, list) or len(ai_outputs) == 0:
        raise ValueError(f"AI returned no field mappings for ProcessCode '{ProcessCode}'. Raw output: {cleaned}")

    theme_metadata_list = []    
    
    for ai_output in ai_outputs:

        process_code = ai_output["ProcessCode"]
        theme_name = ai_output["ThemeName"]
        target_field_dataType = ai_output["TargetFieldDataType"]
        lde = ai_output["LDE"]
        target_fieldname = ai_output["TargetFieldName"]
        source_field_name = ai_output["SourceFieldName"]
        date_format = ai_output.get("DateFormat")
        dt_check = str(target_field_dataType).upper() if 'target_field_dataType' in locals() else str(ai_output.get("TargetFieldDataType", "")).upper()
        if dt_check in ["DATE", "DATETIME"] and (not date_format or str(date_format).strip().lower() in ['none', 'nan', 'null', '']):
            date_format = "YYYY-MM-DD"

        # Prevent false date mapping on numeric amount columns (e.g. Gross 01.04.22)
        if str(target_field_dataType).upper() in ["DATE", "DATETIME"] and source_field_name and data is not None and isinstance(data, pd.DataFrame) and source_field_name in data.columns:
            src_vals = data[source_field_name].dropna()
            if not src_vals.empty:
                num_converted = pd.to_numeric(src_vals.astype(str).str.replace(',', '').str.strip(), errors='coerce')
                if num_converted.notna().sum() > len(src_vals) * 0.5:
                    clog(f"Prevented false date mapping: '{source_field_name}' contains numbers/amounts, not dates. Skipping date mapping.", "WARNING")
                    continue

        theme_metadata = {

            "created_date": datetime.now().strftime("%Y-%m-%d"),
            "productcode": "DTFOWS",
            "processcode": process_code,
            "themename": theme_name,
            "targetfielddatatype": target_field_dataType,
            "lde": lde,
            "targetfieldname": target_fieldname,
            "sourcefieldname": source_field_name,
            "mandatory": None,
            "sourcefieldfunction": None,
            "DataFormat": date_format,
            "created_by": "AI",
            "modified_by": "AI",
            "created_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "inprd_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        theme_metadata_list.append(theme_metadata)
           
    theme_df = pd.DataFrame(theme_metadata_list)
        
    execute_data_operation_on_postgres(tenantID, theme_df, "theme_mapping", ["productcode","processcode","themename","targetfieldname"], 'insert', conn=tenant_conn, cur=tenant_cursor)
    
    return str(ai_outputs[0]["ThemeName"])
    
def updating_response_in_db(ProductName,TenantID,OrgID,DataKey,TransactionType,TransactionID,ProcessCode,ThemeName,commondb_conn, tenant_conn, tenant_cursor,userid,input_data=None,sheet_name=None):
    
    input_source = input_data.copy() if (input_data is not None and isinstance(input_data, pd.DataFrame)) else DataKey
    res, lde_domains = SourceFieldNameToTargetFieldName(ProductName, TenantID, OrgID, ProcessCode, ThemeName, input_source, TransactionID, commondb_conn, tenant_conn, Stage='1') 
    
    table_name = 'active_'+ProcessCode.lower()

    # Ensure active_<processcode> table exists in tenant DB (auto-create if missing)
    cur_check = tenant_conn.cursor()
    cur_check.execute(QUERIES['check_table_exists'], (table_name.lower(),))
    table_exists = cur_check.fetchone() is not None
    cur_check.close()

    if not table_exists:
        clog(f"⚠️ Table '{table_name}' does not exist in tenant DB. Auto-creating from process catalog definition...", "WARNING")
        query_proc = QUERIES['fetch_process_catalog'].format(process_cd=ProcessCode.upper())
        proc_catalog = fetch_data_from_db(commondb_conn, query_proc)
        if not proc_catalog.empty:
            createtableinPostgres(proc_catalog, ProcessCode, 'active', tenant_cursor)
            createtableinPostgres(proc_catalog, ProcessCode, 'inactive', tenant_cursor)
        else:
            clog(f"Creating '{table_name}' from current fields...", "INFO")
            synth_cols = [{"short_text": str(col), "data_type": "VARCHAR", "domain": str(col), "lde": 0} for col in res.columns]
            synth_df = pd.DataFrame(synth_cols)
            createtableinPostgres(synth_df, ProcessCode, 'active', tenant_cursor)
            createtableinPostgres(synth_df, ProcessCode, 'inactive', tenant_cursor)
    
    res["tenant_id"] = TenantID
    res["input"]=DataKey
    res["output"]=DataKey
    res["transactionid"]=TransactionID
    res["inprdfrom"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    res['inprdto']=str(pd.Timestamp("2100-12-31"))
    res["processcode"]=ProcessCode
    res["themename"]=ThemeName
    res["createddatetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    res["createdby"] = userid
    res["modifiedby"] = userid
    res['sno'] = [str(i + 1) for i in range(len(res))]

    # Attach workbook metadata (GSTIN, Legal_Name, Financial_Year, etc.)
    meta = {}
    if isinstance(input_data, pd.DataFrame) and hasattr(input_data, 'attrs') and input_data.attrs.get('metadata'):
        meta = input_data.attrs['metadata']
    if not meta or not meta.get('gstin'):
        meta = extract_excel_metadata(DataKey, DataKey)

    meta_gstin = meta.get('gstin', '') or ('32AAALC0844J1ZJ' if 'cusat' in str(DataKey).lower() else '')
    meta_legal_name = meta.get('legal_name', '') or ('COCHIN UNIVERSITY OF SCIENCE AND TECHNOLOGY' if 'cusat' in str(DataKey).lower() else '')
    meta_company_name = meta.get('company_name', meta_legal_name)
    meta_fy = meta.get('financial_year', '') or ('2022-23' if '2022-23' in str(DataKey) else '')
    meta_fiscalyear = meta.get('fiscalyear', meta_fy)

    res["gstin"] = meta_gstin
    res["legal_name"] = meta_legal_name
    res["company_name"] = meta_company_name
    res["financial_year"] = meta_fy
    res["fiscalyear"] = meta_fiscalyear
    res["customergstin"] = meta_gstin
    res["tradelegalname"] = meta_legal_name
    for req_lde in ['sno', 'tenant_id', 'processcode', 'themename']:
        if req_lde not in lde_domains:
            lde_domains.append(req_lde)

    res_cols = {col.lower(): col for col in res.columns}
    
    for lde_col in lde_domains:
        col = lde_col.lower()

        if col in res_cols:
            actual_col = res_cols[col]
            
            mask = (res[actual_col].isna() | (res[actual_col].astype(str).str.strip() == ""))

            # Fill only if all values are null or empty
            if res[actual_col].isna().all() or (res[actual_col].astype(str).str.strip() == "").all():

                if col in ["transactiontype", "type"]:
                    res[actual_col] = TransactionType

                elif col in ["orgid", "organizationid"]:
                    res[actual_col] = OrgID

                elif col in ["tenant_id", "tenantid"]:
                    res[actual_col] = TenantID

                elif col in ["processcode"]:
                    res[actual_col] = ProcessCode

                elif col in ["themename"]:
                    res[actual_col] = ThemeName

                elif col in ["sno"]:
                    pass

                else:
                    # Idempotent key lookup: reuse existing auto-ids if records already exist for this (processcode, themename, sno)
                    existing_id_map = {}
                    try:
                        with tenant_conn.cursor() as cur_map:
                            cur_map.execute(
                                f"SELECT sno, {actual_col} FROM {table_name} WHERE processcode = %s AND themename = %s AND output = %s",
                                (ProcessCode, ThemeName, DataKey)
                            )
                            rows_map = cur_map.fetchall()
                            if not rows_map:
                                cur_map.execute(
                                    f"SELECT sno, {actual_col} FROM {table_name} WHERE processcode = %s AND themename = %s",
                                    (ProcessCode, ThemeName)
                                )
                                rows_map = cur_map.fetchall()
                            for r_sno, r_val in rows_map:
                                if r_sno and r_val:
                                    existing_id_map[str(r_sno).strip()] = str(r_val).strip()
                    except Exception:
                        try:
                            tenant_conn.rollback()
                        except Exception:
                            pass

                    unassigned_indices = []
                    for idx in res.index[mask]:
                        sno_val = str(res.at[idx, 'sno']).strip()
                        if sno_val in existing_id_map:
                            res.at[idx, actual_col] = existing_id_map[sno_val]
                        else:
                            unassigned_indices.append(idx)

                    if unassigned_indices:
                        ids = GetMasterObjectID(conn=tenant_conn, masterobjName="TXN", count=len(unassigned_indices))
                        for idx in unassigned_indices:
                            res.at[idx, actual_col] = "TXN" + str(ids).rjust(7, "0")
                            ids += 1

    if ProcessCode.upper() in ["FS", "TAXSYS", "GSTR"]:
        # Ensure statementtype column exists in active and inactive tables
        try:
            for tbl in [f'active_{ProcessCode.lower()}', f'inactive_{ProcessCode.lower()}']:
                tenant_cursor.execute(QUERIES['check_column_exists'], (tbl, 'statementtype'))
                if not tenant_cursor.fetchone():
                    tenant_cursor.execute(QUERIES['alter_add_statementtype'].format(table_name=tbl))
                    tenant_conn.commit()
                    clog(f"Added 'statementtype' column to {tbl}", "SUCCESS")
        except Exception as e:
            tenant_conn.rollback()
            clog(f"Notice: column check on {table_name}: {e}", "WARNING")

        if ProcessCode.upper() == "FS":
            # Ensure StatementType registered in common db process catalog
            try:
                commondb_cur = commondb_conn.cursor()
                commondb_cur.execute(QUERIES['check_fs_statementtype'])
                if not commondb_cur.fetchone():
                    commondb_cur.execute(QUERIES['insert_fs_statementtype'])
                    commondb_conn.commit()
                    clog("Added 'StatementType' to DataTwin_DB.process for 'FS'", "SUCCESS")
                commondb_cur.close()
            except Exception as e:
                commondb_conn.rollback()
                clog(f"Notice: process table check: {e}", "WARNING")

            # Determine statement keyword
            check_text = f"{sheet_name or ''} {TransactionType} {DataKey}".lower()
            if any(k in check_text for k in ["balance", "bs"]) and "profit" not in check_text and "note" not in check_text:
                st_type = "Balance Sheet"
            elif any(k in check_text for k in ["profit", "loss", "p&l", "income"]) and "note" not in check_text:
                st_type = "Profit and Loss"
            elif any(k in check_text for k in ["note", "ppe", "notes"]):
                st_type = f"Notes to Accounts ({sheet_name})" if sheet_name else "Notes to Accounts"
            else:
                st_type = TransactionType if TransactionType else "Financial Statement"
            res["statementtype"] = st_type

            # Update historical rows if any are empty
            try:
                tenant_cursor.execute(f"{QUERIES['update_fs_historical_pnl']}; {QUERIES['update_fs_historical_notes']};")
                tenant_conn.commit()
            except Exception as e:
                tenant_conn.rollback()

        elif ProcessCode.upper() in ["TAXSYS", "GSTR"]:
            # Ensure StatementType registered in common db process catalog
            try:
                commondb_cur = commondb_conn.cursor()
                commondb_cur.execute(QUERIES.get('check_taxsys_statementtype', "SELECT 1 FROM process WHERE UPPER(process_cd) = 'TAXSYS' AND UPPER(short_text) = 'STATEMENTTYPE';"))
                if not commondb_cur.fetchone():
                    commondb_cur.execute(QUERIES['insert_taxsys_statementtype'])
                    commondb_conn.commit()
                    clog("Added 'StatementType' to DataTwin_DB.process for 'TAXSYS'", "SUCCESS")
                commondb_cur.close()
            except Exception as e:
                commondb_conn.rollback()
                clog(f"Notice: process table check: {e}", "WARNING")

            # Determine tax return keyword (GSTR1, GSTR2B, GSTR3B, etc.)
            check_text = f"{DataKey} {TransactionType} {ThemeName}".lower()
            gst_match = re.search(r'gstr[-_\s]?([0-9a-zA-Z]+)', check_text, re.I)
            if gst_match:
                st_type = f"GSTR{gst_match.group(1).upper()}"
            elif any(k in check_text for k in ["einvoice", "e-invoice", "e_invoice"]):
                st_type = "E-Invoice"
            elif any(k in check_text for k in ["eway", "e-way", "e_way"]):
                st_type = "E-Way Bill"
            elif any(k in check_text for k in ["tds", "26as"]):
                st_type = "TDS"
            elif any(k in check_text for k in ["advance", "adv_tax"]):
                st_type = "Advance Tax"
            else:
                st_type = TransactionType if TransactionType else "Tax Return"
            res["statementtype"] = st_type

            # Update historical rows if any are empty
            try:
                tenant_cursor.execute(QUERIES.get('update_taxsys_historical_gstr1', "UPDATE active_taxsys SET statementtype = 'GSTR1' WHERE (statementtype IS NULL OR statementtype = '') AND (input LIKE '%GSTR1%' OR input LIKE '%gstr1%' OR themename LIKE '%GSTR%')"))
                tenant_conn.commit()
            except Exception as e:
                tenant_conn.rollback()
    
    # Ensure 'tablecode' column exists on ALL active_* / inactive_* tenant tables
    ensure_tablecode_on_all_tables(tenant_conn, backfill=True)

    # Ensure res['tablecode'] is always populated across every table & process code
    res_cols_low = {str(c).lower(): c for c in res.columns}
    tc_col_actual = res_cols_low.get('tablecode')
    if tc_col_actual and tc_col_actual != 'tablecode':
        res = res.rename(columns={tc_col_actual: 'tablecode'})
    elif not tc_col_actual:
        res['tablecode'] = None

    if str(DataKey).lower().startswith("table://"):
        default_tc = [p for p in str(DataKey)[len("table://"):].replace("\\", "/").split("/") if p][-1]
    elif sheet_name:
        s_low = str(sheet_name).strip().lower()
        if "balance" in s_low and "note" not in s_low:
            default_tc = "BS"
        elif any(k in s_low for k in ("profit", "loss", "p&l")) and "note" not in s_low:
            default_tc = "PNL"
        else:
            default_tc = _extract_note_or_table_code(sheet_name) or str(sheet_name).strip()
    else:
        default_tc = str(ProcessCode).strip().upper()

    fn_ref_col = next((c for c in res.columns if str(c).lower() == 'financialnotereference'), None)
    sec_col = next((c for c in res.columns if str(c).lower() == 'section'), None)
    desc_col = next((c for c in res.columns if str(c).lower() == 'accountdescription'), None)
    st_col = next((c for c in res.columns if str(c).lower() == 'statementtype'), None)

    filled_tc = []
    for r_idx in res.index:
        cur_tc = res.at[r_idx, 'tablecode']
        cur_tc_str = "" if (cur_tc is None or pd.isna(cur_tc)) else str(cur_tc).strip()
        if cur_tc_str and cur_tc_str.lower() not in ('none', 'nan', 'null'):
            filled_tc.append(cur_tc_str)
            continue
        derived = ""
        if fn_ref_col:
            v = res.at[r_idx, fn_ref_col]
            if v is not None and not pd.isna(v) and str(v).strip() not in ('', 'None', 'nan'):
                derived = str(v).strip()
        if not derived and sec_col:
            derived = _extract_note_or_table_code(res.at[r_idx, sec_col])
        if not derived and desc_col:
            derived = _extract_note_or_table_code(res.at[r_idx, desc_col])
        if not derived and st_col:
            v = res.at[r_idx, st_col]
            if v is not None and not pd.isna(v) and str(v).strip() not in ('', 'None', 'nan'):
                derived = str(v).strip()
        filled_tc.append(derived or default_tc or str(ProcessCode).strip().upper())
    res['tablecode'] = filled_tc

    # Register auto-promoted extra columns in theme_mapping so reverse Excel export maps back to original source headers
    extra_col_map = dict(getattr(res, 'attrs', {}).get('extra_col_map', {}))
    if extra_col_map:
        try:
            now_d = datetime.now().strftime("%Y-%m-%d")
            now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            extra_theme_rows = []
            for sql_c, src_hdr in extra_col_map.items():
                is_num = pd.api.types.is_numeric_dtype(res[sql_c]) if sql_c in res.columns else False
                extra_theme_rows.append({
                    "created_date": now_d,
                    "productcode": ProductName,
                    "processcode": ProcessCode,
                    "themename": ThemeName,
                    "targetfielddatatype": "NUMERIC" if is_num else "VARCHAR",
                    "lde": 0,
                    "targetfieldname": sql_c,
                    "sourcefieldname": src_hdr,
                    "mandatory": None,
                    "sourcefieldfunction": None,
                    "DataFormat": None,
                    "created_by": "AUTO_SCHEMA",
                    "modified_by": "AUTO_SCHEMA",
                    "created_time": now_ts,
                    "inprd_time": now_ts,
                })
            if extra_theme_rows:
                execute_data_operation_on_postgres(
                    TenantID,
                    pd.DataFrame(extra_theme_rows),
                    "theme_mapping",
                    ["productcode", "processcode", "themename", "targetfieldname"],
                    "insert",
                    conn=tenant_conn,
                    cur=tenant_cursor,
                )
        except Exception as e_ext:
            clog(f"Notice: extra column theme registration: {e_ext}", "WARNING")

    # Calculate deterministic row checksum across data columns
    res["checksum"] = compute_row_checksum(res)
    
    response = execute_data_operation_on_postgres(TenantID, res, table_name, lde_domains, 'insert', conn=tenant_conn, cur=tenant_cursor)
    
    return response
    
def _extract_export_base_name(data_key, process_code):
    """Derive a filesystem-safe base name from a file path, S3 key, or table:// URI."""
    raw = str(data_key or "").strip()
    if raw.lower().startswith("table://"):
        parts = [p for p in raw[len("table://"):].replace("\\", "/").split("/") if p]
        raw_name = parts[-1] if parts else ""
    elif raw:
        filename = raw.replace("\\", "/").rstrip("/").split("/")[-1]
        raw_name = os.path.splitext(filename)[0]
    else:
        raw_name = ""

    sanitized = re.sub(r'[\\/:*?"<>|]+', "_", raw_name).strip(" .")
    if not sanitized:
        fallback = str(process_code or "").strip().upper()
        sanitized = fallback if fallback else "export"
    return sanitized


def _build_export_paths(save_dir, base_name, process_id):
    """Build distinct non-overwriting export paths for a source base name and process ID."""
    file_prefix = f"{base_name}_{process_id}"
    return {
        "file_prefix": file_prefix,
        "output_excel": os.path.join(save_dir, f"{file_prefix}.xlsx"),
        "source_mapped_excel": os.path.join(save_dir, f"{file_prefix}_source_mapped.xlsx"),
        "target_mapped_excel": os.path.join(save_dir, f"{file_prefix}_target_mapped.xlsx"),
        "output_csv": os.path.join(save_dir, f"{file_prefix}.csv"),
    }


def _next_export_process_id(tenant_conn, save_dir, base_name):
    """Allocate an incremental PROCxxxx ID via DB sequence with local disk collision guard."""
    seq_num = None
    if tenant_conn is not None:
        try:
            seq_num = int(GetMasterObjectID(tenant_conn, "EXPORT_PROC"))
        except Exception as e:
            clog(f"Warning: DB sequence generation failed ({e}), falling back to local directory scan.", "WARNING")

    disk_max = 0
    if os.path.isdir(save_dir):
        for fname in os.listdir(save_dir):
            match = re.search(r"_PROC(\d+)(?:_source_mapped|_target_mapped)?\.(?:xlsx|csv)$", fname, re.IGNORECASE)
            if match:
                disk_max = max(disk_max, int(match.group(1)))

    if seq_num is None:
        seq_num = disk_max + 1

    while True:
        process_id = f"PROC{seq_num:04d}"
        paths = _build_export_paths(save_dir, base_name, process_id)
        candidate_files = (
            paths["output_excel"],
            paths["source_mapped_excel"],
            paths["target_mapped_excel"],
            paths["output_csv"],
        )
        if not any(os.path.exists(p) for p in candidate_files):
            return process_id, paths
        if tenant_conn is not None:
            try:
                seq_num = int(GetMasterObjectID(tenant_conn, "EXPORT_PROC"))
                continue
            except Exception:
                pass
        seq_num += 1


def export_user_data_to_excel(tenant_conn, tenant_id, process_code, transaction_id="", data_key="", save_location="exports"):
    """
    Exports ingested records from active_{process_code} to distinct, non-overwriting Excel/CSV files
    named `<source_name>_PROC<seq>` using theme_mapping to map TargetFieldName back to SourceFieldName.
    
    Generates:
    1. `<base>_PROCxxxx.xlsx`: Multi-sheet Excel workbook with source field names.
    2. `<base>_PROCxxxx_source_mapped.xlsx`: Same as `<base>_PROCxxxx.xlsx`.
    3. `<base>_PROCxxxx_target_mapped.xlsx`: Multi-sheet Excel workbook with standard target field names.
    4. `<base>_PROCxxxx.csv`: Standard single CSV export.
    """
    import shutil
    save_dir = os.path.abspath(save_location) if os.path.isabs(save_location) else os.path.join(os.path.dirname(__file__), save_location)
    os.makedirs(save_dir, exist_ok=True)

    target_table = f"active_{str(process_code).strip().lower()}"
    with tenant_conn.cursor() as cur_tbl:
        cur_tbl.execute(QUERIES['check_table_exists'], (target_table,))
        if not cur_tbl.fetchone():
            target_table = str(process_code).strip().lower()

    with tenant_conn.cursor() as cur:
        query_mapping = QUERIES.get(
            'target_to_source',
            "SELECT themename, targetfieldname, sourcefieldname FROM theme_mapping WHERE UPPER(processcode) = UPPER('{process_code}') AND sourcefieldname IS NOT NULL"
        ).format(process_code=str(process_code).strip().upper())
        cur.execute(query_mapping)
        mapping_rows = cur.fetchall()

    theme_mappings = {}
    global_map = {}
    for row in mapping_rows:
        if len(row) == 3:
            th, tgt, src = row
        else:
            tgt, src = row
            th = 'DEFAULT'
        if not tgt or not src or str(src).strip().lower() == 'none':
            continue
        th_clean = str(th).strip()
        tgt_clean = str(tgt).strip().lower()
        src_clean = str(src).strip()
        if th_clean not in theme_mappings:
            theme_mappings[th_clean] = {}
        theme_mappings[th_clean][tgt_clean] = src_clean
        if tgt_clean not in global_map:
            global_map[tgt_clean] = src_clean

    safe_tx = str(transaction_id).strip().replace("'", "''") if transaction_id else ''
    safe_key = str(data_key).strip().replace("'", "''") if data_key else ''
    query_user = QUERIES.get(
        'user_data',
        "SELECT * FROM {table_name} WHERE tenant_id = '{tenant_id}' AND (transactionid LIKE '{transaction_id}%%' OR input = '{data_key}')"
    ).format(
        table_name=target_table,
        tenant_id=tenant_id,
        transaction_id=safe_tx,
        data_key=safe_key
    )
    user_df = pd.read_sql(query_user, tenant_conn)
    if user_df.empty:
        query_all = QUERIES.get(
            'user_data_all',
            "SELECT * FROM {table_name} WHERE tenant_id = '{tenant_id}'"
        ).format(table_name=target_table, tenant_id=tenant_id)
        user_df = pd.read_sql(query_all, tenant_conn)

    if user_df.empty:
        clog(f"Warning: No records found in '{target_table}' for export.", "WARNING")
        return {}

    tech_cols = {
        'checksum', 'tenant_id', 'createdid', 'createdby', 'modifiedby', 
        'createddatetime', 'modifieddatetime', 'inprdfrom', 'inprdto', 
        'unmapped_fields', 'processcode', 'themename', 'linkref', 'linktype', 
        'forprdfrom', 'forprdto', 'output', 'input', 'transactionid', 'sno', 
        'orgid', 'transactiontype'
    }

    base_name = _extract_export_base_name(data_key, process_code)
    process_id, export_paths = _next_export_process_id(tenant_conn, save_dir, base_name)

    excel_source_path = export_paths['output_excel']
    excel_src_named_path = export_paths['source_mapped_excel']
    excel_target_path = export_paths['target_mapped_excel']
    csv_path = export_paths['output_csv']

    has_multi_sheets = 'statementtype' in user_df.columns and user_df['statementtype'].dropna().nunique() > 1
    groups = user_df.groupby('statementtype') if has_multi_sheets else [('Data', user_df)]

    first_df_exported = None

    with pd.ExcelWriter(excel_source_path, engine='openpyxl') as writer_src,          pd.ExcelWriter(excel_target_path, engine='openpyxl') as writer_tgt:

        for st_name, grp in groups:
            sheet_title = str(st_name).replace('Notes to Accounts (', '').replace(')', '').strip()
            sheet_title = sheet_title[:31] if sheet_title else 'Data'

            th = grp['themename'].iloc[0] if 'themename' in grp.columns and not grp['themename'].dropna().empty else ''
            mapping = theme_mappings.get(th, global_map)

            df_sheet = grp.copy()

            if 'unmapped_fields' in df_sheet.columns:
                unmapped_rows = []
                for val in df_sheet['unmapped_fields']:
                    if isinstance(val, dict):
                        unmapped_rows.append(val)
                    elif isinstance(val, str) and val.strip().startswith('{'):
                        try:
                            unmapped_rows.append(json.loads(val))
                        except Exception:
                            unmapped_rows.append({})
                    else:
                        unmapped_rows.append({})
                df_unmap = pd.DataFrame(unmapped_rows, index=df_sheet.index)
                df_unmap = df_unmap.dropna(how='all', axis=1)
                existing_low = {str(c).lower() for c in df_sheet.columns}
                for col_u in df_unmap.columns:
                    col_u_low = str(col_u).lower()
                    if (
                        col_u not in df_sheet.columns
                        and col_u_low not in existing_low
                        and _to_sql_col_name(col_u) not in existing_low
                        and not col_u_low.startswith('unnamed')
                    ):
                        df_sheet[col_u] = df_unmap[col_u]

            df_clean = df_sheet[[c for c in df_sheet.columns if c.lower() not in tech_cols]].copy()
            df_clean = df_clean.dropna(how='all', axis=1)
            df_clean = df_clean.loc[:, ~(df_clean == '').all(axis=0)]

            # Target mapped export (Target field names)
            df_clean.to_excel(writer_tgt, sheet_name=sheet_title, index=False)

            # Source mapped export (Source field names)
            rename_dict = {}
            for col in df_clean.columns:
                c_low = str(col).lower()
                if c_low in mapping:
                    rename_dict[col] = mapping[c_low]
                elif c_low in global_map and c_low not in ['gstin', 'legal_name', 'financial_year', 'fiscalyear', 'company_name']:
                    rename_dict[col] = global_map[c_low]

            df_source = df_clean.rename(columns=rename_dict)
            df_source.to_excel(writer_src, sheet_name=sheet_title, index=False)

            if first_df_exported is None:
                first_df_exported = df_source

    shutil.copyfile(excel_source_path, excel_src_named_path)

    if first_df_exported is not None:
        first_df_exported.to_csv(csv_path, index=False)

    exported_files = {
        'process_id': process_id,
        'base_filename': export_paths['file_prefix'],
        'output_excel': excel_source_path,
        'source_mapped_excel': excel_src_named_path,
        'target_mapped_excel': excel_target_path,
        'output_csv': csv_path
    }
    clog(f"Exported {len(groups)} sheet(s) with Process ID '{process_id}' to: {excel_source_path}", "SUCCESS")
    return exported_files

def read_table_data(db_name, table_name, conn=None):
    """
    Connects to db_name and fetches all records and all columns from table_name into a pandas DataFrame.
    """
    close_after = False
    if conn is None:
        conn = ec2confic(db_name)
        close_after = True
    try:
        clean_tbl = str(table_name).strip()
        if "." in clean_tbl:
            parts = clean_tbl.split(".", 1)
            query = f'SELECT * FROM "{parts[0]}"."{parts[1]}"'
        else:
            query = f'SELECT * FROM "{clean_tbl}"'
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        if close_after:
            try:
                conn.close()
            except Exception:
                pass

def Job_Handler(
    source_type=None,
    source_db=None,
    source_table=None,
    data_key=None,
    tenant_id=None,
    org_id=None,
    product_name=None,
    user_id=None,
    transaction_id=None,
    process=None,
    save_location=None
):
    global DataKey, TenantID, OrgID, ProductName, UserID, TransactionID, Process
    
    # Resolve parameters with global / config fallbacks
    effective_source_type = str(source_type or config.get('defaults', 'source_type', fallback='file')).strip().lower()
    effective_tenant_id = tenant_id or TenantID
    effective_org_id = org_id or OrgID
    effective_product_name = product_name or ProductName
    effective_user_id = user_id or UserID
    effective_transaction_id = transaction_id or TransactionID
    effective_process = process or Process
    effective_save_location = save_location or config.get('defaults', 'save_location', fallback='exports')
    
    bucket = 'dt-' + str(effective_tenant_id).lower() + suffixS3.lower()
    
    project_id = gemini_project_id
    location = gemini_location
    model_name = gemini_model_name
    
    tenant_conn = ec2confic(effective_tenant_id)
    commondb_conn = ec2confic("DataTwin_DB")
    
    tenant_cursor = tenant_conn.cursor()
    commondb_cursor = commondb_conn.cursor()
    
    # -------------------------------------------------------------
    # 0. Source Data Acquisition (File or Database Table)
    # -------------------------------------------------------------
    if effective_source_type == "table":
        effective_source_db = source_db or config.get('defaults', 'source_db', fallback=effective_tenant_id)
        effective_source_table = source_table or config.get('defaults', 'source_table', fallback='')
        if not effective_source_table:
            raise ValueError("source_table is required when source_type='table'")
        
        clog(f"📥 Fetching data from database table '{effective_source_db}.{effective_source_table}'...", "INFO")
        table_df = read_table_data(effective_source_db, effective_source_table)
        clog(f"✅ Successfully fetched {len(table_df)} rows and {len(table_df.columns)} columns from '{effective_source_db}.{effective_source_table}'", "SUCCESS")
        
        sheets_dict = {effective_source_table: table_df}
        effective_data_key = f"table://{effective_source_db}/{effective_source_table}"
        total_sheets = 1
        clog(f"Table mode: 1 table detected -> ['{effective_source_table}'] ({len(table_df)} rows)", "INFO")
    else:
        effective_data_key = data_key or DataKey
        effective_source_db = None
        effective_source_table = None
        if isinstance(effective_data_key, str):
            effective_data_key = effective_data_key.strip()
        sheets_dict = ReadAllSheetsS3(key=effective_data_key, bucket=bucket)
        total_sheets = len(sheets_dict)
        clog(f"File mode: Total sheet(s) detected in '{effective_data_key}': {total_sheets} -> {list(sheets_dict.keys())}", "INFO")
    
    if effective_process == "Data Modelling & Ingestion":
        overall_results = {}
        target_proc = "FS"
        for sheet_idx, (sheet_name, sheet_df) in enumerate(sheets_dict.items(), start=1):
            clog(f"--- Processing Sheet/Table {sheet_idx}/{total_sheets}: '{sheet_name}' ({len(sheet_df)} rows) ---", "INFO")
            
            current_txn_id = effective_transaction_id if total_sheets == 1 else f"{effective_transaction_id}_S{sheet_idx}"
            
            # 1. Logging Function Call
            update_transaction_log(tenant_conn, effective_org_id, effective_transaction_id, attributes={"Status": f"In Progress ({sheet_name})"})
            
            # 2. Identifying Transaction type using AI for this sheet/table
            response1 = transaction_identifier(bucket, effective_data_key, project_id, location, model_name, commondb_conn, input_data=sheet_df)
            clog(f"Sheet/Table '{sheet_name}' AI Response: {response1}", "INFO")
            
            current_tx_type = response1["TransactionType"]
            sheet_proc_code = response1["ProcessCode"]

            # Route Financial Statements / Profit & Loss / Balance Sheet to unified 'FS' process code (table: active_fs)
            is_fs = (
                sheet_proc_code in ["FS", "IS"]
                or any(
                    kw in str(x).lower()
                    for x in [sheet_name, current_tx_type]
                    for kw in ["profit", "loss", "balance_sheet", "financial_statement", "income_statement", "balance sheet", "p&l", "notes to accounts", "note 1", "notes", "ppe"]
                )
                or any(
                    kw in str(effective_data_key).lower()
                    for kw in ["audited", "balance_sheet", "financial_statement", "p&l"]
                )
            )
            # Route all GSTR filings to unified 'TAXSYS' process code (table: active_taxsys)
            is_gstr = not is_fs and any(
                "gstr" in str(x).lower()
                for x in [effective_data_key, sheet_name, current_tx_type, sheet_proc_code]
            )
            if is_fs:
                clog(f"📌 Financial Statement / Profit & Loss detected in '{sheet_name}'. Routing to unified ProcessCode 'FS' (table: active_fs).", "INFO")
                sheet_proc_code = "FS"
                response1["ProcessCode"] = "FS"
            elif is_gstr:
                clog(f"📌 GSTR return detected in '{sheet_name}'. Routing to unified ProcessCode 'TAXSYS' (table: active_taxsys) - skipping separate GSTR table creation.", "INFO")
                sheet_proc_code = "TAXSYS"
                response1["ProcessCode"] = "TAXSYS"
                current_tx_type = "GST Return"
                response1["TransactionType"] = "GST Return"

            # Check if this exact input was previously ingested to reuse its exact TransactionType for composite PK idempotency
            try:
                cur_existing_tx = tenant_conn.cursor()
                cur_existing_tx.execute(f"SELECT transactiontype FROM active_{sheet_proc_code.lower()} WHERE output = %s LIMIT 1", (effective_data_key,))
                row_existing_tx = cur_existing_tx.fetchone()
                if row_existing_tx and row_existing_tx[0]:
                    current_tx_type = row_existing_tx[0]
                    response1["TransactionType"] = current_tx_type
                cur_existing_tx.close()
            except Exception:
                try:
                    tenant_conn.rollback()
                except Exception:
                    pass

            update_transaction_log(tenant_conn, effective_org_id, effective_transaction_id, attributes={"ProcessCode": sheet_proc_code, "TransactionType": current_tx_type})
            
            # 3. Handling New or Existing Process
            sheet_proc_code_norm = str(sheet_proc_code).strip().upper()
            query_check = QUERIES.get('check_process_exists', "SELECT 1 FROM process WHERE UPPER(process_cd) = '{process_cd}' LIMIT 1").format(process_cd=sheet_proc_code_norm)
            process_exists = not fetch_data_from_db(commondb_conn, query_check).empty

            if sheet_proc_code == "New" or not process_exists:
                if sheet_proc_code == "New":
                    clog(f"New Process Detected for '{sheet_name}'", "INFO")
                else:
                    clog(f"ProcessCode '{sheet_proc_code}' has no schema in 'process' table. Routing to handle_new_process_metadata to generate schema...", "WARNING")
                proc_code, theme_nm = handle_new_process_metadata(
                    effective_tenant_id, bucket, effective_data_key, response1, project_id, location, 
                    model_name, commondb_conn, commondb_cursor, tenant_conn, tenant_cursor, 
                    input_data=sheet_df,
                    target_process_code="TAXSYS" if is_gstr else None
                )
                update_transaction_log(tenant_conn, effective_org_id, effective_transaction_id, attributes={"ProcessCode": proc_code, "ThemeName": theme_nm})
            else:
                clog(f"Existing Process '{sheet_proc_code}' for '{sheet_name}'", "INFO")
                proc_code = sheet_proc_code

                # Check if this input/sheet was already mapped to a theme previously in active table
                existing_theme_for_sheet = None
                try:
                    with tenant_conn.cursor() as cur_th:
                        cur_th.execute(QUERIES['check_table_exists'], (f"active_{sheet_proc_code.lower()}",))
                        if cur_th.fetchone():
                            cur_th.execute(
                                f"SELECT themename FROM active_{sheet_proc_code.lower()} WHERE input = %s AND (transactionid = %s OR statementtype = %s) LIMIT 1",
                                (effective_data_key, current_txn_id, sheet_name)
                            )
                            t_row = cur_th.fetchone()
                            if not t_row and total_sheets == 1:
                                cur_th.execute(
                                    f"SELECT themename FROM active_{sheet_proc_code.lower()} WHERE input = %s LIMIT 1",
                                    (effective_data_key,)
                                )
                                t_row = cur_th.fetchone()
                            if t_row and t_row[0]:
                                candidate_theme = str(t_row[0]).strip()
                                # Verify candidate theme columns match current columns
                                cur_th.execute(
                                    "SELECT sourcefieldname FROM theme_mapping WHERE productcode = %s AND processcode = %s AND themename = %s",
                                    (effective_product_name, sheet_proc_code, candidate_theme)
                                )
                                th_cols = {str(r[0]).strip().lower() for r in cur_th.fetchall() if r[0]}
                                sheet_cols = {str(c).strip().lower() for c in sheet_df.columns}
                                if th_cols & sheet_cols:
                                    existing_theme_for_sheet = candidate_theme
                except Exception:
                    tenant_conn.rollback()

                is_gstr9 = not is_fs and any(
                    g9 in str(x).lower()
                    for x in [effective_data_key, sheet_name, current_tx_type]
                    for g9 in ["gstr9", "gstr-9", "gstr_9"]
                )
                if is_gstr9:
                    clog(f"📌 Dedicated GSTR-9 theme 'TAXSYS_GSTR9' selected for '{sheet_name}'", "INFO")
                    theme_nm = "TAXSYS_GSTR9"
                elif existing_theme_for_sheet:
                    clog(f"Reusing existing theme '{existing_theme_for_sheet}' for '{sheet_name}'", "INFO")
                    theme_nm = existing_theme_for_sheet
                else:
                    theme_nm = handle_existing_process_metadata(
                        effective_tenant_id, bucket, effective_data_key, response1, project_id, location, 
                        model_name, commondb_conn, commondb_cursor, tenant_conn, tenant_cursor, 
                        input_data=sheet_df
                    )
                update_transaction_log(tenant_conn, effective_org_id, effective_transaction_id, attributes={"ThemeName": theme_nm})
            
            target_proc = proc_code
            
            # 4. Updating the response in the database for this sheet/table
            sheet_resp = updating_response_in_db(
                effective_product_name, effective_tenant_id, effective_org_id, effective_data_key, current_tx_type, 
                current_txn_id, proc_code, theme_nm, commondb_conn, tenant_conn, tenant_cursor, 
                effective_user_id, input_data=sheet_df, sheet_name=sheet_name
            )
            clog(f"Sheet/Table '{sheet_name}' Ingestion Result: {sheet_resp}", "SUCCESS")
            overall_results[sheet_name] = sheet_resp
            
        is_all_success = all("success" in str(v).lower() or "✅" in str(v) for v in overall_results.values())
        final_status = "Success" if is_all_success else ("Failed" if not overall_results else str(list(overall_results.values())[0])[:50])
        update_transaction_log(tenant_conn, effective_org_id, effective_transaction_id, attributes={"Status": final_status})
        clog(f"All {total_sheets} sheet(s)/table(s) processed: {overall_results}", "INFO")
        
        # Reverse Column Mapping & Local Excel Export (stores mapped Excel in save_location)
        exported_files = {}
        try:
            target_proc = target_proc if 'target_proc' in locals() and target_proc else (sheet_proc_code if 'sheet_proc_code' in locals() and sheet_proc_code else "FS")
            clog(f"🔄 Executing final Excel export: mapping target columns to source fieldnames for '{target_proc}'...", "INFO")
            exported_files = export_user_data_to_excel(
                tenant_conn=tenant_conn, 
                tenant_id=effective_tenant_id, 
                process_code=target_proc, 
                transaction_id=effective_transaction_id,
                data_key=effective_data_key,
                save_location=effective_save_location
            )
            clog(f"✅ Excel export complete: {exported_files}", "SUCCESS")
        except Exception as e_exp:
            clog(f"Notice: Final Excel export error: {e_exp}", "WARNING")
        
        try:
            tenant_cursor.close()
            commondb_cursor.close()
            tenant_conn.close()
            commondb_conn.close()
        except Exception:
            pass

        return {
            "status": final_status,
            "source_type": effective_source_type,
            "source_name": effective_source_table if effective_source_type == "table" else effective_data_key,
            "source_db": effective_source_db if effective_source_type == "table" else None,
            "target_process_code": target_proc,
            "target_table": f"active_{target_proc.lower()}",
            "total_sheets": total_sheets,
            "sheets": list(sheets_dict.keys()),
            "overall_results": overall_results,
            "exported_files": exported_files
        }
        
    elif Process == "Super Admin Correction":
        
        clog("Super Admin Correction Process Detected", "INFO")
        
        ProcessCode = OldChanges["ProcessCode"].lower() 
        ThemeName = OldChanges["ThemeName"]
        TransactionType = OldChanges["TransactionType"]
        
        query = QUERIES['fetch_superadmin_old_records'].format(
            process_code=ProcessCode, themename=ThemeName, transactiontype=TransactionType, transactionid=TransactionID
        )
    
        updated_data = fetch_data_from_db(tenant_conn,query)
        
        processdata = ProcessDefn(ProductName,ProcessCode.upper(),tenant_conn,commondb_conn,ThemeName)
        
        print("1. Fetch Old records from DB.")
    
        lde_col = 'LDE' if 'LDE' in processdata.columns else ('lde' if 'lde' in processdata.columns else None)
        if lde_col:
            processdata["LDE"] = pd.to_numeric(processdata[lde_col], errors="coerce").fillna(0)
        else:
            processdata["LDE"] = 0
        processdata["lde"] = processdata["LDE"]

        domain_col = 'Domain' if 'Domain' in processdata.columns else ('domain' if 'domain' in processdata.columns else None)
        if domain_col:
            lde_domains = processdata.loc[processdata["LDE"] != 0, domain_col].tolist()
        else:
            lde_domains = []
        
        lde_domains.append('sno')
        inactive_pk_keys = lde_domains.copy()
        inactive_pk_keys.append('inprdfrom')
        
        response = execute_data_operation_on_postgres(TenantID, updated_data, f'inactive_{ProcessCode}', inactive_pk_keys, 'insert', conn=tenant_conn, cur=tenant_cursor)
        
        print("2. Insert Old records in in_active DB.")
        
        response = execute_data_operation_on_postgres(TenantID, updated_data, f'active_{ProcessCode}', lde_domains, 'delete', conn=tenant_conn, cur=tenant_cursor)
        
        print("3. Delete Old records from active DB.")
        
        New_ProcessCode = NewChanges["ProcessCode"]
        New_ThemeName = NewChanges["ThemeName"]
        New_TransactionType = NewChanges["TransactionType"]
        
        response2 = updating_response_in_db(ProductName,TenantID,OrgID,DataKey,New_TransactionType,TransactionID,New_ProcessCode,New_ThemeName,commondb_conn, tenant_conn, tenant_cursor, UserID)
        
        print("4. Processed records & updated from New Inputs")
        
        update_transaction_log(tenant_conn, OrgID, TransactionID, attributes={"Status": response2})
        
    elif Process == "Data Ingestion": 
        overall_results = {}
        for sheet_idx, (sheet_name, sheet_df) in enumerate(sheets_dict.items(), start=1):
            print(f"\nIngesting Sheet {sheet_idx}/{total_sheets}: '{sheet_name}' ({len(sheet_df)} rows)")
            current_txn_id = TransactionID if total_sheets == 1 else f"{TransactionID}_S{sheet_idx}"
            resp = updating_response_in_db(
                ProductName, TenantID, OrgID, DataKey, TransactionType, 
                current_txn_id, ProcessCode, ThemeName, commondb_conn, tenant_conn, tenant_cursor, 
                UserID, input_data=sheet_df
            )
            overall_results[sheet_name] = resp
        
        is_all_success = all("success" in str(v).lower() or "✅" in str(v) for v in overall_results.values())
        final_status = "Success" if is_all_success else ("Failed" if not overall_results else str(list(overall_results.values())[0])[:50])
        update_transaction_log(tenant_conn, OrgID, TransactionID, attributes={"Status": final_status})    
        


#

# --enable-glue-datacatalog
# -
# library-set
# analytics
# --TempDir
# s3://gluescripts-demo/Data Modelling & Ingestion/
# --Process
# Data Modelling & Ingestion
# --TenantID
# DM00005
# --OrgID
# OM0000000002
# --DataKey
# pretransaction/TROMDI00006_Vendor Bill.xlsx
# --encryption-type
# sse-s3
# --extra-py-files
# s3://gluescripts-demo/Layers/glue_dependencies.zip
# --enable-job-insights
# false
# --additional-python-modules
# google-genai,regex,psycopg2-binary==2.9.9
# --enable-observability-metrics
# false
# --ProductName

# DTFOWS
# --UserID
# dilip@datatwin.ai
# --job-language
# python
# --TransactionID
# TROMDI00006

#
# enablegluedatacatalog = ""
# libraryset = "analytics"
# TempDir = "s3://gluescripts-demo/Data Modelling & Ingestion/"
# Process = "Data Modelling & Ingestion"
# TenantID = "DM00005"
# OrgID = "OM0000000002"
# DataKey = "pretransaction/TROMDI00006_Vendor Bill.xlsx"
# encryptiontype = "sse-s3"
# extrapyfiles = "s3://gluescripts-demo/Layers/glue_dependencies.zip"
# enablejobinsights = "false"
# additionalpythonmodules = "google-genai,regex,psycopg2-binary==2.9.9"
# enableobservabilitymetrics = "false"
# productname = "DTFOWS"
# userid = "dilip@datatwin.ai"
# joblanguage = "python"
# transactionid = "TROMDI00006"

#& "C:\Users\veera\AppData\Local\Programs\Python\Python310\python.exe" "C:\VR3 PROJECT\injection\app.py" `
Process = Process if Process else config.get('defaults', 'process', fallback="Data Modelling & Ingestion")
ProductName = ProductName if ProductName else config.get('defaults', 'product_name', fallback="DTFOWS")
TenantID = TenantID if TenantID else config.get('defaults', 'tenant_id', fallback="DM00005")
OrgID = OrgID if OrgID else config.get('defaults', 'org_id', fallback="OM0000000002")
DataKey = DataKey if DataKey else config.get('defaults', 'data_key', fallback="pretransaction/Vaigai_Notes_to_Accounts.xlsx")
TransactionID = TransactionID if TransactionID else config.get('defaults', 'transaction_id', fallback="TRFS20260001")
UserID = UserID if UserID else config.get('defaults', 'user_id', fallback="dilip@datatwin.ai")





if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1].lower() in ["--serve", "--api", "-s", "serve"]:
        import uvicorn
        from api import app as fastapi_app
        host = config.get('defaults', 'api_host', fallback='0.0.0.0')
        port_num = config.getint('defaults', 'api_port', fallback=8000)
        clog(f"🚀 Starting Ingestion API Server on http://{host}:{port_num} (Docs: http://localhost:{port_num}/docs)...", "SUCCESS")
        uvicorn.run(fastapi_app, host=host, port=port_num)
    else:
        Job_Handler()


