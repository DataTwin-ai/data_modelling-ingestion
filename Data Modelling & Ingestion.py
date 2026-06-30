# Packages
import sys
from boto3 import client
import io
from io import BytesIO
from google import genai
from google.genai import types
from google.oauth2 import service_account
import json
from datetime import datetime
import json
import json
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


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

# Parse arguments with safe checks
process = get_argument_value("--Process")
print("Process:",process)
tenantID = get_argument_value("--TenantID")
dataKey = get_argument_value("--DataKey")
promptKey = get_argument_value("--PromptKey")
orgID = get_argument_value("--OrgID")
transactionType = get_argument_value("--TransactionType")

tableName=tenantID+suffixDb

bucket='dt-'+tenantID.lower()+suffixS3.lower()

athena=client("athena",region_name=regionName)

dynamoDB=client("dynamodb",region_name=regionName)

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

def update_transaction_log(conn,txnID, operation, attributes={}):

    "PostgreSQL Transaction Log Interface"

    print()

    cursor = conn.cursor()

    if operation == "StatusCheck":

        cursor.execute(
            "SELECT * FROM transaction_log WHERE transactionid = %s",
            (txnID,)
        )
        resp = cursor.fetchone()

    elif operation in ["PUT", "ADD"]:

        cursor.execute(
            "SELECT id FROM transaction_log WHERE transactionid = %s",
            (txnID,)
        )
        existing = cursor.fetchone()

        if existing:

            cursor.execute(
                """
                UPDATE transaction_log
                SET status    = %s,
                    createdat = %s
                WHERE transactionid = %s
                """,
                (attributes["Status"], datetime.now().isoformat(), txnID)
            )

        else:

            cursor.execute(
                """
                INSERT INTO transaction_log (
                    transactionid, status, createdat,
                    tenantid, orgid, datakey
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    txnID,
                    attributes["Status"],
                    datetime.now().isoformat(),
                    attributes["TenantID"],
                    attributes["OrgID"],
                    DataKey
                )
            )

        resp = {"Status": attributes["Status"], "TransactionID": txnID}

    elif operation == "CREATE":

        cursor.execute(
            """
            INSERT INTO transaction_log (
                transactionid, status, createdat,
                tenantid, orgid, datakey
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                txnID,
                attributes["Status"],
                datetime.now().isoformat(),
                attributes["TenantID"],
                attributes["OrgID"],
                DataKey
            )
        )

        resp = {"Status": attributes["Status"], "TransactionID": txnID}
        
    else:

        resp = {"Status": "Error", "Message": f"Unsupported operation: {operation}"}    

    conn.commit()
    cursor.close()
    conn.close()

    return resp

def execute_data_operation_on_postgres(tenantID, df, table_name, pk_keys, operation, conn=None, cur=None):

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
            response = f"✅ Upserted {len(df)} rows into '{table_name}'"

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

########################################### AI/ML Functions ###########################################

def ReadS3(key,bucket,readTxtTsv=False):

    "read s3 file and return dataframe based on the parameter passed."    

    try:  

        file=s3.get_object(Bucket=bucket,Key=key)
        key_lower = key.lower()

        if ".json" not in key:

            ioObj=io.BytesIO(file['Body'].read())

        else:

            ioObj=file['Body'].read().decode('utf-8')

            # Special Condition For Json-serde format

            if ioObj[0]!="[" and ioObj[-1]!="]":

                ioObj="["+ioObj+"]"
                
                ioObj=ioObj.replace("\n",",") 

        if ".csv" in key:  

            fileDF=pd.read_csv(ioObj, encoding='utf-8', nrows=10)
            fileDF.to_csv(index=False) 
        
        elif '.tsv' in key or readTxtTsv==True:

            fileDF=pd.read_csv(ioObj, encoding='utf-8',sep="\t")
            fileDF.to_csv(index=False) 
        
        elif ".xlsx" in key:
            
            fileDF = pd.read_excel(ioObj, engine='openpyxl', nrows=10) 
            fileDF.to_csv(index=False) 

        elif key_lower.endswith((".xls", ".xlsx")):
            try:
                fileDF = pd.read_excel(ioObj, engine='openpyxl', nrows=10)
            except Exception:
                fileDF = pd.read_excel(ioObj, engine='xlrd', nrows=10)    

        elif ".xl" in key:

            if key[-4:]=='.xls':

                engine='xlrd'
            
            else:

                engine='openpyxl'

            fileDF=pd.read_excel(ioObj,engine=engine, nrows=10)
            fileDF.to_csv(index=False) 

        elif ".XL" in key:

            if key[-4:]=='.XLS':

                engine='xlrd'
            
            else:

                engine='openpyxl'

            fileDF=pd.read_excel(ioObj,engine=engine, nrows=10) 
            fileDF.to_csv(index=False)    

        elif ".json" in key:

            fileDF=pd.read_json(ioObj,orient='records')
            fileDF.to_csv(index=False) 
        
        elif ".txt" in key:

            fileDF=ioObj.read().decode("utf-8")

        else:

            fileDF = None    
                
    except UnicodeDecodeError:

        file=s3.get_object(Bucket=bucket,Key=key)

        if ".json" not in key:

            ioObj=io.BytesIO(file['Body'].read())

            print(ioObj)

        else:

            # Special Condition For Json-serde format

            ioObj=file['Body'].read().decode('ISO-8859-1')

            if ioObj[0]!="[" and ioObj[-1]!="]":

                ioObj="["+ioObj+"]"
                
                ioObj=ioObj.replace("\n",",")   

        if ".csv" in key:  

            fileDF=pd.read_csv(ioObj, encoding='ISO-8859-1', nrows=10)
            fileDF.to_csv(index=False) 
        
        elif '.tsv' in key or readTxtTsv==True:

            fileDF=pd.read_csv(ioObj, encoding='ISO-8859-1',sep="\t")
            fileDF.to_csv(index=False) 
        
        elif ".xlsx" in key:

            fileDF = pd.read_excel(ioObj, engine='openpyxl', nrows=10)
            fileDF.to_csv(index=False) 

        elif ".xl" in key:

            if key[-4:]=='.xls':

                engine='xlrd'
            
            else:

                engine='openpyxl'

            fileDF=pd.read_excel(ioObj,engine=engine, nrows=10)
            fileDF.to_csv(index=False)     
        
        elif ".json" in key:

            fileDF=pd.read_json(ioObj,orient='records',encoding='ISO-8859-1') 
            fileDF.to_csv(index=False) 

        elif ".txt" in key:

            fileDF=ioObj.read().decode('ISO-8859-1')

        else:

            fileDF = None    
    
    if ".txt" in key and readTxtTsv ==False:

        return fileDF
    
    else:

        fileDF.rename(columns=lambda x: x.strip(),inplace=True)

        fileDF=fileDF.dropna(subset=fileDF.columns,how='all')

        return fileDF

def get_excel_date_format_from_s3(bucket, key):

    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    excel_data = BytesIO(
        response["Body"].read()
    )

    workbook = load_workbook(
        excel_data,
        data_only=False
    )

    sheet = workbook.active

    for row in sheet.iter_rows():

        for cell in row:

            value = cell.value

            if isinstance(value, datetime):

                fmt = cell.number_format.lower()

                fmt = fmt.replace("-", "/")
                fmt = fmt.replace(".", "/")

                if "dd" in fmt and "mm" in fmt and "yyyy" in fmt:

                    if fmt.index("dd") < fmt.index("mm"):
                        return "DD/MM/YYYY"

                    else:
                        return "MM/DD/YYYY"

                elif "yyyy" in fmt:
                    if fmt.index("yyyy") < fmt.index("mm"):
                        return "YYYY/MM/DD"

    return None    

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

def transaction_identifier(bucket,DataKey,project_id,location,model_name,commondb_conn):

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

        ### Input Data:
        {data_str}
        """

    response = calling_aiV2(client, model_name, system_instruction, content)

    cleaned = response.strip('`')          # removes backticks
    cleaned = cleaned.replace('json\n', '')  # removes 'json' label

    data = json.loads(cleaned)

    return data

def updating_response_in_db(ProductName,TenantID,OrgID,DataKey,TransactionType,TransactionID,ProcessCode,ThemeName):
    
    res = ""

def Metadata_Generator(transaction_id,dataKey,orgID,transactionType):
    
    conn = ec2confic(tenantID)
    cursor = conn.cursor()

    bucket='dt-'+TenantID.lower()+suffixS3.lower()

    api_key = "AIzaSyCS3vVTFR26djNgUlyxuP6FdQLGqaOwFHo"

    project_id = "datatwinai-dev"

    location = "us-central1"

    model_name = "gemini-2.5-flash"

    system_instruction = """
    You are an enterprise ERP metadata generator.
    """

    model = get_ai_model(
        bucket,
        api_key,
        project_id,
        location,
        model_name,
        system_instruction
    )

    df = ReadS3(
        key=dataKey,
        bucket=bucket
    )


    transaction_type = transactionType

    date_format = get_excel_date_format_from_s3(
        bucket,
        dataKey
    )

    if date_format:
     date_format = date_format.upper()

    cursor.execute("""
        SELECT DISTINCT process_cd
        FROM process
        ORDER BY process_cd
    """)

    existing_codes = [
        row[0]
        for row in cursor.fetchall()
    ]

    print(existing_codes)

    promptKey = "Prompt/Vertex ai/ERP_Generator.txt"
    raw_prompt = ReadS3(key=promptKey, bucket=bucket)

    prompt = raw_prompt.replace("{existing_codes}", str(existing_codes)) \
                       .replace("{date_format}", str(date_format)) \
                       .replace("{transaction_type}", str(transaction_type)) \
                       .replace("{df.head(3).to_string(index=False)}", df.head(3).to_string(index=False))

    try:
        response = calling_aiV2(
            model,
            prompt
        )
    except Exception:
        raise Exception("Gemini Error")

    cleaned = response.strip('`')

    cleaned = cleaned.replace(
        'json\n',
        ''
    )

    try:
        ai_outputs = json.loads(cleaned)
    except Exception:
        raise Exception("JSON Parse Error")

    final_json_list = []

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

        final_json = {
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
            "acronym": f"{process_code}_{short_text}"
        }

        theme_metadata = {

            "ProcessCode": process_code,
            "ThemeName": process_name,
            "TargetFieldDataType": data_type,
            "LDE": lde,
            "TargetFieldName": short_text,
            "SourceFieldName": source_field_name,
            "Mandatory": None,
            "DataFormat": (
                date_format
                if (
                    "date" in source_field_name.lower()
                    or
                    "date" in short_text.lower()
                    or
                    data_type == "DATE"
                )
                else None
            )
        }

        final_json_list.append(final_json)

        theme_metadata_list.append(theme_metadata)

    print(
        json.dumps(
            final_json_list,
            indent=4
        )
    )

    print(
        json.dumps(
            theme_metadata_list,
            indent=4
        )
    )

    return {
        "transaction_id": transaction_id,
        "final_json_list": final_json_list,
        "theme_metadata_list": theme_metadata_list
    }
  
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
        ai_outputs = json.loads(cleaned)
    except Exception:
        raise Exception("JSON Parse Error")

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
        
    execute_data_operation_on_postgres(tenantID, process_df, "process", ["product_cd","process_cd","short_text"], 'insert', conn=commondb_conn, cur=commondb_cursor)
    execute_data_operation_on_postgres(tenantID, theme_df, "theme_mapping", ["productcode","processcode","themename","targetfieldname"], 'insert', conn=tenant_conn, cur=tenant_cursor)
    
    return str(ai_output["ProcessCode"]), str(ai_output["ProcessName"])
    
def handle_existing_process_metadata(tenantID, bucket, DataKey, response1, project_id,location, model_name, commondb_conn, commondb_cursor, tenant_conn,tenant_cursor):
    
    ProcessCode = response1["ProcessCode"]
    transaction_type = response1["TransactionType"]
    
    promptKey = "Prompt/Vertex ai/prompt.EP.txt"

    system_instruction = ""

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
    
    content = prompt_data.replace("{existing_TargetFieldName}", str(existing_short_texts)) \
                       .replace("{existing_theme_names_list}", str(existing_themenames)) \
                       .replace("{sample_df_data}", data.head(3).to_string(index=False)) \
                       .replace("{transactionType}", str(transaction_type)) \
                       .replace("{process_code}", str(ProcessCode))    
                       
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
    
def Job_Handler():

    bucket='dt-'+TenantID.lower()+suffixS3.lower()
    
    project_id = "datatwinai-dev"
    location = "us-central1"
    model_name = "gemini-2.5-flash"
    
    tenant_conn = ec2confic(TenantID)
    commondb_conn = ec2confic("DataTwin_DB")
    
    tenant_cursor = tenant_conn.cursor()
    commondb_cursor = commondb_conn.cursor()
    
    # #1.Logging Function Call
    # update_transaction_log(tenant_conn, TransactionID, operation="PUT", attributes={"Status": "Started", "TenantID": TenantID, "OrgID": OrgID})
    
    # #2.Identifying Transaction type using AI
    # response1 = transaction_identifier(bucket,DataKey,project_id,location,model_name,commondb_conn)
    
    # update_transaction_log(tenant_conn, TransactionID, operation="PUT",attributes={"ProcessCode" : response1["ProcessCode"], "TransactionType": response1["TransactionType"]})
    
    response1 = {'TransactionType': 'Contract', 'ProcessCode': 'BT3I'}
    
    #3.
    if response1["ProcessCode"] == "New":

        print("New Process Detected")
        
        processCd, themeName = handle_new_process_metadata(TenantID, bucket, DataKey, response1, project_id, location, model_name, commondb_conn, commondb_cursor, tenant_conn, tenant_cursor)

    else:

        print("Existing Process")
        
        themeName = handle_existing_process_metadata(TenantID, bucket, DataKey, response1, project_id, location, model_name, commondb_conn, commondb_cursor, tenant_conn, tenant_cursor)
    
    #4.
    
    TransactionType = 'PurchaseOrder'
    ProcessCode = 'BT3I'
    ThemeName = 'POV1'
        
    #response2 = updating_response_in_db(ProductName,TenantID,OrgID,DataKey,TransactionType,TransactionID,ProcessCode,ThemeName)



Process = "Data Modelling & Ingestion"
ProductName = "DTFOWS",
TenantID = "TID00002"
OrgID = "OM0000000002"
DataKey = "pretransaction/purchase-order-data.xlsx"
TransactionID = "TRONDI00226",
UserID = "dilip@datatwin.ai"


if __name__ == "__main__":
    Job_Handler()
