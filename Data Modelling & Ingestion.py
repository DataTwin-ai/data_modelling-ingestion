# Packages
import logging
import sys
from vertexai.generative_models import Part
from boto3 import client
import io
from io import BytesIO
from google import genai
from google.genai import types
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import json
from datetime import datetime, time
import time
import json
import json
import psycopg2
from psycopg2.extras import Json, execute_values
import pandas as pd
import uuid
import pandas as pd
import numpy as np
import regex as re
from psycopg2 import sql
import ast
from pathlib import Path
from PIL import Image
import base64
import PyPDF2
from json_repair import repair_json


env='development'
#env="production"
logLevel=1

if env=='development':
    regionName='us-east-1'
    suffixDb='-DEV'
    ec2IP='44.206.150.114'
    suffixS3='-dev' 
    port=5433
elif env=='production':
    regionName='ap-south-1'
    suffixDb=''
    ec2IP="43.204.93.19"
    suffixS3='-prod'
    port=5432
else:
    regionName='us-east-1'
    suffixDb='-DEV'
    ec2IP='44.206.150.114'
    suffixS3='-dev'   
    port=5433 

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
# Process = get_argument_value("--Process")
# print("Process:", Process)
# ProductName = get_argument_value("--ProductName")
# TenantID = get_argument_value("--TenantID")
# OrgID = get_argument_value("--OrgID")
# DataKey = get_argument_value("--DataKey")
# TransactionID = get_argument_value("--TransactionID")
# UserID = get_argument_value("--UserID")
# OldChanges = get_argument_value("--OldChanges")
# OldChanges = parse_changes(get_argument_value("--OldChanges"))
# print(repr(OldChanges))
# NewChanges = parse_changes(get_argument_value("--NewChanges"))

# TransactionType = get_argument_value("--TransactionType")
# ThemeName = get_argument_value("--ThemeName")
# ProcessCode = get_argument_value("--ProcessCode")

s3=client("s3",region_name=regionName)

######################################## Database Functions ###########################################

def ec2confic(tenantID):
    "connects to ec2 instance and returns connection object."
    if tenantID != "DataTwin_DB":
        tenantID = tenantID.lower()  
    else:
        tenantID = tenantID
    conn = psycopg2.connect(
        host = ec2IP,
        port = port,
        dbname = tenantID,
        user = "postgres",
        password = "dt@2025"
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
        return pd.DataFrame(), {"Status": "Error", "Message": str(e)}

def execute_data_operation_on_postgres(tenantID, df, table_name, pk_keys, operation, conn=None, cur=None):
    
    pk_keys = [key.lower() for key in pk_keys]
    df.columns = df.columns.str.lower()

    # Replace NaN/NaT with None
    df = df.where(pd.notna(df), None)
    df = df.applymap(
        lambda x: (
            None if pd.isna(x) or (isinstance(x, str) and x.strip() == "NaT") else x
        )
    )

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
            delete_sql = f"DELETE FROM {table_name} WHERE " + " AND ".join([f"{pk} = %s" for pk in pk_keys])
            cur.executemany(delete_sql, pk_values)
            conn.commit()
            response = f"✅ Deleted {len(pk_values)} record(s) from {table_name}"

        elif operation.lower() == 'insert':
            cols = ', '.join(df.columns)

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
            
            #df = df.drop_duplicates(subset=pk_keys, keep='last')

            values = [clean_row(row) for row in df.itertuples(index=False)]

            dup_rows = df[df.duplicated(subset=pk_keys, keep=False)]
            if not dup_rows.empty:
                print("⚠️ Duplicate PK rows found before insert:")
                print(dup_rows)
            
            # UPSERT: insert or update if PK conflict
            update_cols = [col for col in df.columns if col not in pk_keys]
            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
            
            #conflict_clause = f"ON CONFLICT ({', '.join(pk_keys)}) DO UPDATE SET {set_clause}"
            conflict_clause = f"ON CONFLICT ON CONSTRAINT {table_name}_pkey DO UPDATE SET {set_clause}"

            insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES %s {conflict_clause}"

            execute_values(cur, insert_sql, values)
            conn.commit()
            response = "Data Successfully loaded into destination table"

        elif operation.lower() == 'select':
            sel_pk_keys=pk_keys.copy()
            sel_pk_keys.append('themename')
            sel_pk_keys.remove('sno')
            pk_values = list({
                tuple(row[pk] for pk in sel_pk_keys)
                for _, row in df.iterrows()
            })
            where_clause = " AND ".join([f"{pk} = %s" for pk in sel_pk_keys])
            select_sql = f"SELECT * FROM {table_name} WHERE {where_clause}"

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
            update_sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

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

        query = """
        INSERT INTO masterobjectsequencing (
            pk,
            sk,
            last_updated_id,
            last_updated_on
        )
        VALUES (
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (pk, sk)
        DO UPDATE
        SET
            last_updated_id = masterobjectsequencing.last_updated_id + %s,
            last_updated_on = EXCLUDED.last_updated_on
        RETURNING
            CASE
                WHEN last_updated_id = %s THEN 0
                ELSE last_updated_id - %s
            END AS previous_last_updated_id;
        """

        with conn.cursor() as cursor:

            cursor.execute(
                query,
                (
                    pk,
                    sk,
                    count,
                    current_time,
                    count,
                    count,
                    count
                )
            )

            previous_last_updated_id = cursor.fetchone()[0]

        conn.commit()

        return previous_last_updated_id+1

    except Exception as e:

        conn.rollback()
        raise Exception(f"Error generating Object ID: {str(e)}")

def createtableinPostgres(process, processCode, tableCatg, tenant_cursor):
    
    filtered_df = process[process['lde'] != 0]
    sorted_df = filtered_df.sort_values('lde')

    Pks = ", ".join(str(name).lower() for name in sorted_df['domain'])
    addPks = ', sno' if tableCatg == 'active' else ', sno, inprdfrom'
    Pks += addPks
    theme = 'processcode, themename'
    Pks = f"{theme}, {Pks}"

    table_name = tableCatg + '_' + processCode.lower()

    column_names = process['short_text'].str.lower().tolist()
    data_types = process['data_type'].str.lower().tolist()

    column_names.extend(['themename', 'processcode', 'checksum', 'input', 'output','unmapped_fields'])
    data_types.extend(['VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR', 'VARCHAR','JSONB'])

    if 'transactionid' not in column_names:
        column_names.append('transactionid')
        data_types.append('VARCHAR')

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
        'datetime': 'timestamp'
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

    create_query = f"""DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
      {columns_str},
      PRIMARY KEY ({Pks})
    );"""

    tenant_cursor.execute(create_query)
    tenant_cursor.connection.commit()
    response = f"✅ Created table '{table_name}'"

    return print(response)

def update_transaction_log(tenant_conn, OrgID, TransactionID, attributes: dict):
    if not attributes:
        return False

    try:
        with tenant_conn.cursor() as cur:

            # Convert attribute keys to lowercase column names
            update_data = {
                str(column).lower(): value
                for column, value in attributes.items()
            }

            set_clause = sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(column))
                for column in update_data.keys()
            )

            query = sql.SQL("""
                UPDATE {table}
                SET {set_clause}
                WHERE {org_col} = %s
                  AND {txn_col} = %s
            """).format(
                table=sql.Identifier("transaction_log"),
                set_clause=set_clause,
                org_col=sql.Identifier("orgid"),
                txn_col=sql.Identifier("transactionid")
            )

            values = list(update_data.values()) + [OrgID, TransactionID]

            cur.execute(query, values)

        tenant_conn.commit()
        return True

    except Exception as e:
        tenant_conn.rollback()
        raise e

########################################### PDT Functions ###########################################

def ThemeData(productCode,processCode,themeName,tenant_conn):

    query = """
        SELECT *
        FROM theme_mapping
        WHERE productcode = %s
          AND processcode = %s
          AND themename = %s
    """

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

    query = """
        SELECT *
        FROM theme_mapping
        WHERE productcode = %s
          AND processcode = %s
          AND themename = %s
    """

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

    "Retrieves Process Definition From S3"

    query = """
        SELECT *
        FROM public."process"
        WHERE process_cd <> 'process_cd'
        AND process_cd = '%s'
    """ % processCode
    
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
                ["", "nan", "none", "pd.na", "<na>", "nat"],
                np.nan
            )

            converted = pd.to_numeric(normalized, errors="coerce")

            invalid_mask = converted.isna() & normalized.notna()

            if invalid_mask.any():

                append_error(
                    inputdata,
                    invalid_mask,
                    f"Invalid Number Format: {source_field}"
                )

                invalid_values = (
                    normalized[invalid_mask]
                    .astype(str)
                    .unique()
                    .tolist()
                )

                exceptions.append(
                    f"Column '{source_field}' contains invalid numeric values: {invalid_values}"
                )

        # =====================================================
        # DATE VALIDATION
        # =====================================================
        elif data_type in ["DATE", "DATETIME"]:

            if not date_format:
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

        format=list(themeData.loc[themeData["SourceFieldName"]==dateCol,"DataFormat"])[0]

        formatName=list(metaInfo.loc[metaInfo["Domain"]==srcToTargetMapping[dateCol],"DataType"])[0]

        internalFormat=format

        dateTimeAttrs=dateTimeParmsRX.findall(internalFormat)

        for attr in dateTimeAttrs:

            internalFormat=internalFormat.replace(attr,dateTimeMapping[attr])      

        try:

            data[dateCol]=pd.to_datetime(data[dateCol],format=internalFormat)

            if formatName=="DATE":

                data[dateCol]=[str(x)[:10] for x in list(data[dateCol])]
            
            else:

                data[dateCol]=[str(x) for x in list(data[dateCol])]

        except Exception as E:

            exceptions.append("Date Format Issue [column:%s]; [PDM:%s]; [Exact Error:%s]"%(dateCol,str(format),E))

            return exceptions

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
                    ["", " ", "None", "none", "NULL", "null", "NaN", "nan", None],
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
    
    processdata["lde"] = pd.to_numeric(processdata["lde"], errors="coerce")

    lde_domains = processdata.loc[processdata["lde"].fillna(0) != 0,"Domain"].tolist()

    # Read Data
    if isinstance(input,pd.DataFrame):

        inputdata=input 

    else:          

        try:

            inputdata = ReadS3(key=input, bucket=bucketName)

        except Exception as e:    

            exceptions.append("Failed/File Content Not Matching With The Extension:"+input)
            raise Exception(exceptions)
    
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

    if isinstance(inputdata,pd.DataFrame) and Stage == "1" and processCode == "BT3I":
    
        inputdata['uuid'] = [str(uuid.uuid4()) for _ in range(len(inputdata))]

        return inputdata, lde_domains
    
    else : 

        return inputdata, lde_domains

def fetch_file_from_s3(bucket_name, s3_key):
    """Fetch a file from S3 and return its in-memory stream."""
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        return BytesIO(response['Body'].read())
    except Exception as e:
        print(f"Error fetching file from S3: {e}key:{s3_key}")
        return None

########################################### AI/ML Functions ###########################################

def split_pdf_into_pages(pdf_stream):
    """Splits a PDF into individual pages as in-memory streams."""
    try:
        pdf_reader = PyPDF2.PdfReader(pdf_stream)
        pages = []
        for page in pdf_reader.pages:
            pdf_writer = PyPDF2.PdfWriter()
            pdf_writer.add_page(page)
            page_stream = BytesIO()
            pdf_writer.write(page_stream)
            page_stream.seek(0)
            pages.append(page_stream)
        return pages
    except Exception as e:
        logging.error(f"Error splitting PDF into pages: {e}")
        return []

def split_jpeg_into_pages(jpeg_stream):
    try:
        image = Image.open(jpeg_stream)
        pages = []

        if hasattr(image, 'n_frames') and image.n_frames > 1:
            for i in range(image.n_frames):
                image.seek(i)
                page_stream = BytesIO()
                image.save(page_stream, format="JPEG")
                page_stream.seek(0)
                pages.append(page_stream)
        else:
            page_stream = BytesIO()
            image.save(page_stream, format="JPEG")
            page_stream.seek(0)
            pages.append(page_stream)

        print(f"Split successful. {len(pages)} pages processed.")
        return pages
    except Exception as e:
        logging.error(f"Error splitting JPEG into pages: {e}")
        return []
    
def split_png_into_pages(png_stream):

    try:
        image = Image.open(png_stream)
        pages = []

        if hasattr(image, 'n_frames') and image.n_frames > 1:
            # Handle multi-frame/animated PNG
            for i in range(image.n_frames):
                image.seek(i)
                page_stream = BytesIO()
                image.save(page_stream, format="PNG")
                page_stream.seek(0)
                pages.append(page_stream)
        else:
            # Handle single-frame PNG
            page_stream = BytesIO()
            image.save(page_stream, format="PNG")
            page_stream.seek(0)
            pages.append(page_stream)

        print(f"Split successful. {len(pages)} pages processed.")
        return pages
    except Exception as e:
        logging.error(f"Error splitting PNG into pages: {e}")
        return []    

def process_single_page(pdf_page_stream, text_part, model, generation_config, safety_settings, Input_Type):

    """Process a single page and return the response."""
    try:
        print("Starting to process a single page. Input_Type='%s'", Input_Type)

        # Encode the page data in Base64.
        pdf_data = pdf_page_stream.getvalue()
        logging.debug("Read %d bytes from the pdf_page_stream.", len(pdf_data))

        encoded_pdf_data = base64.b64encode(pdf_data).decode("utf-8")
        logging.debug("Encoded PDF data length: %d", len(encoded_pdf_data))

        # Create a Part object from the encoded data based on the input type.
        if Input_Type == "pdf":
            document_part = Part.from_data(mime_type="application/pdf", data=encoded_pdf_data)
            print("Created Part with mime_type='application/pdf'.")
        elif Input_Type == "jpeg":
            document_part = Part.from_data(mime_type="image/jpeg", data=encoded_pdf_data)
            print("Created Part with mime_type='image/jpeg'.")
        elif Input_Type == "png":
            document_part = Part.from_data(mime_type="image/png", data=encoded_pdf_data)
            print("Created Part with mime_type='image/png'.")
        else:
            logging.error("Unsupported Input format: %s", Input_Type)
            return ""

        # Convert the text into a Part object as well.
        text_part_obj = Part.from_text(text=text_part)
        logging.debug("Created Part from text_part with length=%d.", len(text_part))

        # Generate content from the model using both document and text parts.
        print("Calling model.generate_content with streaming enabled.")
        responses = model.generate_content(
            [document_part, text_part_obj],
            generation_config=generation_config,
            safety_settings=safety_settings,
            stream=True,
        )

        # Accumulate the response text.
        page_response = ""
        for response in responses:
            page_response += response.text
        
        print("Finished generating content for this page. Response length: %d", len(page_response))
        return page_response

    except Exception as e:
        logging.exception("Error processing page: %s", e)
        return ""

def ReadS3(key, bucket, readTxtTsv=False):
    """
    Read S3 file and return a pandas DataFrame (or string for raw .txt).
    Handles encoding fallbacks, stream pointer rewinds, and dual Excel engines.
    """
    try:
        file = s3.get_object(Bucket=bucket, Key=key)
        key_lower = key.lower()

        # 1. Read bytes ONCE into memory to prevent S3 stream exhaustion
        raw_bytes = file['Body'].read()

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

def transaction_identifier(bucket,DataKey,project_id,location,model_name,commondb_conn,TransactionType):

    promptKey = "Prompt/Vertex ai/classification.txt"

    system_instruction = "You are an Advanced Financial Controller specialized in Intelligent Document Processing (IDP). You process financial data with 100% precision, mapping input to the correct database schema."

    client = get_ai_client(bucket,project_id,location)

    data = ReadS3(key=DataKey,bucket=bucket)

    prompt_data = ReadS3(key=promptKey,bucket=bucket)

    query ='''
        select * from processcodes_knowledge
        '''
    
    knowledge_data = fetch_data_from_db(commondb_conn,query)
    
    knowledge_dict = knowledge_data.to_dict(orient = "records")
    
    data_str = data.head(3).to_string(index=False)
    
    full_prompt = f"{prompt_data}{knowledge_dict}"

    content = f"""
        {full_prompt}

        ### Transaction Type:
        {TransactionType}
        ### Input Data:
        {data_str}
        """

    response = calling_aiV2(client, model_name, system_instruction, content)

    cleaned = response.strip('`')          # removes backticks
    cleaned = cleaned.replace('json\n', '')  # removes 'json' label

    data = json.loads(cleaned)

    return data

def handle_new_process_metadata(tenantID, bucket, DataKey, response1, project_id,location, model_name, commondb_conn, commondb_cursor, tenant_conn,tenant_cursor):
    
    transaction_type = response1["TransactionType"]
    
    promptKey = "Prompt/Vertex ai/ERP_Generator.txt"

    system_instruction = ""

    client = get_ai_client(bucket,project_id,location)

    data = ReadS3(key=DataKey,bucket=bucket)

    prompt_data = ReadS3(key=promptKey,bucket=bucket)
    
    query ='''
        SELECT DISTINCT process_cd
        FROM process
        ORDER BY process_cd
        '''
    existing_codes = fetch_data_from_db(commondb_conn,query)
    
    content = prompt_data.replace("{existing_codes}", str(existing_codes)) \
                       .replace("{transaction_type}", str(transaction_type)) \
                       .replace("{df.head(3).to_string(index=False)}", data.head(3).to_string(index=False))
                       
    response = calling_aiV2(client, model_name, system_instruction, content)    
    
    cleaned = response.strip('`')

    cleaned = cleaned.replace(
        'json\n',
        ''
    )

    try:
        ai_outputs_raw = json.loads(cleaned)
    except Exception:
        raise Exception("JSON Parse Error")
    
    ai_outputs = ai_outputs_raw["Metadata"]
    
    processcode_knowledge = ai_outputs_raw["ProcessCodeKnowledge"]

    process_metadata_list = []

    theme_metadata_list = []

    for ai_output in ai_outputs:

        process_code = ai_output["ProcessCode"]
        if process_code in existing_codes:
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
        date_format = ai_output["DateFormat"]

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
    processcode_knowledge_df = pd.DataFrame([processcode_knowledge])
    
    ProcessCode = str(ai_output["ProcessCode"])
    ThemeName = str(ai_output["ProcessName"])
        
    execute_data_operation_on_postgres(tenantID, process_df, "process", ["product_cd","process_cd","short_text"], 'insert', conn=commondb_conn, cur=commondb_cursor)
    execute_data_operation_on_postgres(tenantID, theme_df, "theme_mapping", ["productcode","processcode","themename","targetfieldname"], 'insert', conn=tenant_conn, cur=tenant_cursor)
    execute_data_operation_on_postgres(tenantID, processcode_knowledge_df, "processcodes_knowledge", ["ProcessCode"], 'insert', conn=commondb_conn, cur=commondb_cursor)
    
    createtableinPostgres(process_df, ProcessCode, 'active', tenant_cursor)
    createtableinPostgres(process_df, ProcessCode, 'inactive', tenant_cursor)
    
    return ProcessCode, ThemeName
    
def handle_existing_process_metadata(tenantID, bucket, DataKey, response1, project_id,location, model_name, commondb_conn, commondb_cursor, tenant_conn,tenant_cursor):
    
    ProcessCode = response1["ProcessCode"]
    transaction_type = response1["TransactionType"]
    
    promptKey = "Prompt/Vertex ai/prompt.EP.txt"

    client = get_ai_client(bucket,project_id,location)

    data = ReadS3(key=DataKey,bucket=bucket)

    prompt_data = ReadS3(key=promptKey,bucket=bucket)
    
    query =f'''
        SELECT DISTINCT themename
        FROM theme_mapping
        where processcode = '{ProcessCode}'
        ORDER BY themename
        '''
    existing_themenames = fetch_data_from_db(tenant_conn,query)
    
    query =f'''
        SELECT DISTINCT short_text, data_type
        FROM process
        where process_cd = '{ProcessCode}'
        ORDER BY short_text
        '''
    existing_short_texts = fetch_data_from_db(commondb_conn,query) 
    
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
                            .replace("{existing_theme_names_list}", str(existing_themenames['themename'].tolist())) \
                            .replace("{sample_df_data}", data.head(3).to_string(index=False))
    
    system_instruction = """You are a strict deterministic data validation engine and an expert business analyst. 
    Your task is to match Excel source columns against an existing JSON array containing target fields. 
    Return ONLY the final processed raw JSON array. No markdown wrappers like ```json, no conversational text."""
                       
    response = calling_aiV2(client, model_name, system_instruction, content)    
    
    cleaned = response.strip('`')

    cleaned = cleaned.replace(
        'json\n',
        ''
    )

    try:
        ai_outputs = json.loads(cleaned)
    except Exception:
        raise Exception("JSON Parse Error")

    theme_metadata_list = []    
    
    for ai_output in ai_outputs:

        process_code = ai_output["ProcessCode"]
        theme_name = ai_output["ThemeName"]
        target_field_dataType = ai_output["TargetFieldDataType"]
        lde = ai_output["LDE"]
        target_fieldname = ai_output["TargetFieldName"]
        source_field_name = ai_output["SourceFieldName"]
        date_format = ai_output["DateFormat"]

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
    
    return str(ai_output["ThemeName"])
    
def updating_response_in_db(ProductName,TenantID,OrgID,DataKey,TransactionType,TransactionID,ProcessCode,ThemeName,commondb_conn, tenant_conn, tenant_cursor,userid):
    
    res, lde_domains = SourceFieldNameToTargetFieldName(ProductName, TenantID, OrgID, ProcessCode, ThemeName, DataKey, TransactionID, commondb_conn, tenant_conn, Stage='1') 
    
    res_cols = {col.lower(): col for col in res.columns}
    
    for lde_col in lde_domains:
        col = lde_col.lower()

        if col in res_cols:
            actual_col = res_cols[col]
            
            mask = (res[actual_col].isna() |(res[actual_col].astype(str).str.strip() == ""))

            # Fill only if all values are null or empty
            if res[actual_col].isna().all() or (res[actual_col].astype(str).str.strip() == "").all():

                if col in ["transactiontype", "type"]:
                    res[actual_col] = TransactionType

                elif col in ["orgid", "organizationid"]:
                    res[actual_col] = OrgID

                else:
                    row_cnt = len(res.index[mask])
                    
                    if row_cnt > 0:
                    
                        ids = GetMasterObjectID(conn=tenant_conn,masterobjName="TXN",count=row_cnt) 
                        
                        for idx in res.index[mask]:
                            res.at[idx, actual_col] = "TXN" + str(ids).rjust(7, "0")
                            ids += 1

    table_name = 'active_'+ProcessCode.lower()
    
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
    lde_domains.append('sno')
    
    response = execute_data_operation_on_postgres(TenantID, res, table_name, lde_domains, 'insert', conn=tenant_conn, cur=tenant_cursor)
    
    return response

def excel_handler(bucket_name, key, project_id, location, model_name):
    file_extension = Path(key).suffix.lower()

    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
    except Exception as error:
        raise RuntimeError(
            f"Unable to read the S3 file: {bucket_name}/{key}. Error: {error}"
        ) from error

    excel_bytes = response["Body"].read()

    if not excel_bytes:
        raise ValueError("The Excel file in S3 is empty.")

    # Select the appropriate Excel engine.
    if file_extension in [".xlsx", ".xlsm"]:
        engine = "openpyxl"
    elif file_extension == ".xls":
        engine = "xlrd"
    else:
        engine = "pyxlsb"

    excel_file = pd.ExcelFile(io.BytesIO(excel_bytes), engine=engine)
    sheet_names = excel_file.sheet_names

    promptKey = "Prompt/Vertex ai/Cleaning_Excel.txt"
    system_instruction = (
        "You are an expert Data Pipeline Architect and Document Parsing Assistant. "
        "Your task is to analyze document structures and return raw JSON strictly matching instructions."
    )

    client = get_ai_client(bucket_name, project_id, location)
    prompt_data = ReadS3(key=promptKey, bucket=bucket_name)

    processed_sheets = {}

    for sheet_number, sheet_name in enumerate(sheet_names, start=1):
        raw_df = excel_file.parse(
            sheet_name=sheet_name, header=None, dtype=object
        )
        total_rows = raw_df.shape[0]

        if raw_df.empty:
            print(f"Sheet {sheet_name} is empty")
            continue

        # Keep index=True so LLM sees exact row numbers
        first_50_rows = raw_df.head(50).fillna("").to_csv(index=True)

        last_start_index = max(total_rows - 50, 0)
        last_50_rows = (
            raw_df.iloc[last_start_index:].fillna("").to_csv(index=True)
        )

        content = f"""
            {prompt_data}

            ### Input Data (Total Rows: {total_rows})
            --- FIRST 50 ROWS ---
            {first_50_rows}

            --- LAST 50 ROWS ---
            {last_50_rows}
            """

        response = calling_aiV2(
            client, model_name, system_instruction, content
        )

        # Cleanup markdown formatting around JSON
        cleaned = (
            response.strip()
            .replace("```json", "")
            .replace("```", "")
            .strip('`')
            .strip()
        )

        parsed_json = json.loads(cleaned)

        metadata = parsed_json.get("metadata", {})
        table_boundaries = parsed_json.get("table_boundaries", {})

        header_row_index = table_boundaries.get("header_row_index", None)
        skip_top_rows = table_boundaries.get(
            "skip_top_rows", []
        )  # Rows AFTER header
        skip_bottom_rows = table_boundaries.get(
            "skip_bottom_rows", []
        )  # Bottom rows

        cleaned_df = raw_df.copy()

        if header_row_index is not None:
            if isinstance(header_row_index, list):
                h_idx = header_row_index[0]
            else:
                h_idx = header_row_index

            # 1. Set column names from header row
            new_headers = (
                cleaned_df.iloc[h_idx].fillna("").astype(str).str.strip().tolist()
            )

            # 2. Slice dataframe starting immediately AFTER the header row
            cleaned_df = cleaned_df.iloc[h_idx + 1 :].copy()
            cleaned_df.columns = new_headers

            # 3. Drop specified extra rows after header AND bottom footer rows
            rows_to_drop = set(skip_top_rows + skip_bottom_rows)
            valid_rows_to_drop = [
                idx for idx in rows_to_drop if idx in cleaned_df.index
            ]

            cleaned_df = cleaned_df.drop(index=valid_rows_to_drop).reset_index(
                drop=True
            )

        # 4. Attach metadata directly to the DataFrame attributes
        for key, value in metadata.items():
            cleaned_df[key] = value

        processed_sheets[sheet_name] = cleaned_df

    return processed_sheets

def extraction(s3_bucket, DataKey, project_id, location, model_name):
    
    promptKey = "Prompt/Vertex ai/Extraction.txt"
    Input_Type = Path(DataKey).suffix.lower()
    credential_key = "VertexAI/credential.json"

    try:
        # -----------------------------------------------------
        # Read service-account JSON from S3
        # -----------------------------------------------------
        response = s3.get_object(
            Bucket=s3_bucket,
            Key=credential_key
        )

        raw_json = response["Body"].read().decode("utf-8")
        sa_info = json.loads(raw_json)

        # Confirm that this is a service-account credential file.
        if sa_info.get("type") != "service_account":
            raise ValueError(
                "VertexAI/credential.json is not a valid service-account credential file."
            )

        # -----------------------------------------------------
        # Explicitly add the Vertex AI OAuth scope
        # -----------------------------------------------------
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]

        credentials = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=scopes
        )

        credentials.refresh(Request())

        print(
            "Google authentication successful for:",
            credentials.service_account_email
        )

        # -----------------------------------------------------
        # Initialize Gen AI client
        # -----------------------------------------------------
        client = genai.Client(
            vertexai=True,
            project=project_id,
            location=location,
            credentials=credentials
        )

        # Define strict system instructions to keep keys uniform across rows
        system_instruction = (
            "Extract all visible information and structured tabular data from the document. "
            "Output must be a valid JSON object strictly in this structure: {'Tables': [{...}]}. "
            "CRITICAL: Every dictionary object inside the 'Tables' array MUST contain identical key names. "
            "If a key/value is missing for a specific row, set its value to null or empty string ''."
        )

        config = types.GenerateContentConfig(
            system_instruction=system_instruction,
            response_mime_type="application/json",
            max_output_tokens=65536,  # Raised to allow long document generations without truncation
            temperature=0.1,
            safety_settings=[
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold=types.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold=types.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    threshold=types.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                ),
                types.SafetySetting(
                    category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                    threshold=types.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                ),
            ]
        )

        file_stream = fetch_file_from_s3(s3_bucket, DataKey)
        text_file_stream = fetch_file_from_s3(s3_bucket, promptKey)

        if not file_stream or not text_file_stream:
            raise ValueError("Failed to fetch the document or prompt from S3.")

        text_data = text_file_stream.read().decode("utf-8")

        mime_mapping = {
            ".pdf": "application/pdf",
            ".jpeg": "image/jpeg",
            ".jpg": "image/jpeg",
            ".png": "image/png"
        }

        target_mime = mime_mapping.get(Input_Type)
        if not target_mime:
            raise ValueError(f"Unsupported file type: {Input_Type}")

        if Input_Type == ".pdf":
            pdf_pages = split_pdf_into_pages(file_stream)
        elif Input_Type in (".jpeg", ".jpg"):
            pdf_pages = split_jpeg_into_pages(file_stream)
        elif Input_Type == ".png":
            pdf_pages = split_png_into_pages(file_stream)
        else:
            raise ValueError(f"Unsupported file type: {Input_Type}")

        if not pdf_pages:
            return {"Tables": []}

        total_pages = len(pdf_pages)
        print(f"Total pages to process: {total_pages}")

        document_response = s3.get_object(Bucket=s3_bucket, Key=DataKey)
        document_bytes = document_response["Body"].read()

        document_part = types.Part.from_bytes(
            data=document_bytes,
            mime_type=target_mime
        )

        response = client.models.generate_content(
            model=model_name,
            contents=[document_part, text_data],
            config=config
        )

        if not response.text:
            return {"Tables": []}

        # -----------------------------------------------------
        # Robust Parsing: Standard JSON attempt with repair fallback
        # -----------------------------------------------------
        try:
            return json.loads(response.text)
        except json.JSONDecodeError as decode_err:
            logging.warning(
                "JSONDecodeError caught: %s. Attempting repair with json_repair...",
                decode_err
            )
            repaired_text = repair_json(response.text)
            return json.loads(repaired_text)

    except Exception as error:
        logging.exception("Error during document extraction: %s", error)

        return {
            "Tables": [],
            "Error": str(error)
        }

def documentHandler(bucket, DataKey, project_id, location, model_name):

    if DataKey.lower().endswith(('.xlsx', '.xls', '.xlsm', '.xlsb')):
        
        data = excel_handler(bucket, DataKey, project_id, location, model_name)
        
        saved_key = f"posttransaction/{TransactionID}.xlsx"
        
        
        
    elif DataKey.lower().endswith(('.pdf', '.img', '.png', '.jpeg', '.jpg')):
        
        data = extraction(bucket, DataKey, project_id, location, model_name)
        
        saved_key = f"posttransaction/{TransactionID}.json"
        
    else:
        raise ValueError(f"Unsupported file extension for key: {DataKey}")

    return saved_key   
    
def Job_Handler():
    
    bucket='dt-'+TenantID.lower()+suffixS3.lower()
    
    project_id = "datatwinai-dev"
    location = "us-central1"
    model_name = "gemini-2.5-flash"
    
    tenant_conn = ec2confic(TenantID)
    commondb_conn = ec2confic("DataTwin_DB")
    
    tenant_cursor = tenant_conn.cursor()
    commondb_cursor = commondb_conn.cursor()
    
    data = documentHandler(bucket,DataKey, project_id, location, model_name)
    
    if Process == "Data Modelling & Ingestion":
        
        global TransactionType
    
        #1.Logging Function Call
        update_transaction_log(tenant_conn, OrgID, TransactionID, attributes={"Status": "In Progress"})
        
        #2.Identifying Transaction type using AI
        response1 = transaction_identifier(bucket,DataKey,project_id,location,model_name,commondb_conn,TransactionType)
        
        update_transaction_log(tenant_conn, OrgID, TransactionID, attributes={"ProcessCode": response1["ProcessCode"], "TransactionType": response1["TransactionType"]})
        
        TransactionType = response1["TransactionType"]  
        
        #3. Handling New or Existing Process
        if response1["ProcessCode"] == "New":

            print("New Process Detected")
            
            ProcessCode, ThemeName = handle_new_process_metadata(TenantID, bucket, DataKey, response1, project_id, location, model_name, commondb_conn, commondb_cursor, tenant_conn, tenant_cursor)
            
            update_transaction_log(tenant_conn, OrgID, TransactionID, attributes={"ProcessCode": ProcessCode, "ThemeName": ThemeName})

        else:

            print("Existing Process")
            ProcessCode = response1["ProcessCode"]  
            
            ThemeName = handle_existing_process_metadata(TenantID, bucket, DataKey, response1, project_id, location, model_name, commondb_conn, commondb_cursor, tenant_conn, tenant_cursor)
            
            update_transaction_log(tenant_conn, OrgID, TransactionID, attributes={"ThemeName": ThemeName})
        
        #4.Updating the response in the database          
        response2 = updating_response_in_db(ProductName,TenantID,OrgID,DataKey,TransactionType,TransactionID,ProcessCode,ThemeName,commondb_conn, tenant_conn, tenant_cursor, UserID)
        
        update_transaction_log(tenant_conn, OrgID, TransactionID, attributes={"Status": response2})
        
    elif Process == "Super Admin Correction":
        
        print("Super Admin Correction Process Detected")
        
        ProcessCode = OldChanges["ProcessCode"].lower() 
        ThemeName = OldChanges["ThemeName"]
        TransactionType = OldChanges["TransactionType"]
        
        query =f'''
        select * from active_{ProcessCode} where themename = '{ThemeName}' and transactiontype = '{TransactionType}' and transactionid = '{TransactionID}'
        '''
    
        updated_data = fetch_data_from_db(tenant_conn,query)
        
        processdata = ProcessDefn(ProductName,ProcessCode.upper(),tenant_conn,commondb_conn,ThemeName)
        
        print("1. Fetch Old records from DB.")
    
        processdata["LDE"] = pd.to_numeric(processdata["LDE"], errors="coerce")

        lde_domains = processdata.loc[processdata["LDE"].fillna(0) != 0,"Domain"].tolist()
        
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
        
        response2 = updating_response_in_db(ProductName,TenantID,OrgID,DataKey,TransactionType,TransactionID,ProcessCode,ThemeName,commondb_conn, tenant_conn, tenant_cursor, UserID)
        
        print("1. Processed records & updated from the Inputs")
        
        update_transaction_log(tenant_conn, OrgID, TransactionID, attributes={"Status": response2})    
        
        
Process = "Data Modelling & Ingestion"
ProductName = "DTFOWS"
TenantID = "DM00005"
OrgID = "OM0000000002"
DataKey = "pretransaction/TROMDI00006_Vendor Bill.xlsx"
TransactionID = "TROMDI00006"
UserID = "dilip@datatwin.ai"
TransactionType = ""
AdditionalInfo = {
}
ValidSheetNames = []


# Process = "Super Admin Correction"
# ProductName = "DTFOWS"
# TenantID = "TID00002"
# OrgID = "OM0000000002"
# DataKey = "pretransaction/purchase-order-data.xlsx"
# TransactionID = "TRONDI00226"
# UserID = "dilip@datatwin.ai"
# OldChanges = {
#     "ProcessCode": "CON",
#     "ThemeName": "Contract",
#     "TransactionType": "Contract"
# }
# NewChanges = {
#     "ProcessCode": "BT3I",
#     "ThemeName": "sap_POV1",
#     "TransactionType": "PurchaseOrder"
# }

# Process = "Data Modelling & Ingestion V2"
# ProductName = "DTFOWS"
# TenantID = "TID00002"
# OrgID = "OM0000000002"
# DataKey = "pretransaction/purchase-order-data.xlsx"
# TransactionID = "TRONDI00227"
# UserID = "dilip@datatwin.ai"
# TransactionType = "PurchaseOrder"
# ThemeName = "sap_POV1"
# ProcessCode = "BT3I"

# Process = "Data Modelling & Ingestion"
# ProductName = "DTFOWS"
# TenantID = "DM00005"
# OrgID = "OM0000000002"
# DataKey = "pretransaction/TROMDI00006_Vendor Bill.xlsx"
# TransactionID = "TROMDI00006"
# UserID = "dilip@datatwin.ai"
# TransactionType = ""
# AdditionalInfo = {
#     "BankName": "Axis Bank",
#     "AccountNumber": "1234567890"
# }
# ValidSheetNames = ["Sheet0"]

if __name__ == "__main__":
    
    Job_Handler()
