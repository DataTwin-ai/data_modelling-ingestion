# Packages
import sys
from boto3 import client
from botocore.exceptions import ClientError
import io
import base64
import PyPDF2
from io import BytesIO
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting, HarmBlockThreshold
import json
import time
from datetime import date, datetime
import re
from PIL import Image
from google.oauth2 import service_account
import os
import boto3
import json
import re
import json
import hashlib
import psycopg2
import pandas as pd
import awswrangler as wr
import numpy as np
import uuid


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

##############################PDT Functions##################################

def ThemeDataV2(productCode,tenantID,processCode,themeName):

    bucketName = 'dt-' + tenantID.lower() + suffixS3.lower()

    themeNamekey = f"ThemeMapping/{productCode}/{processCode}/{themeName}.csv"

    themeData = ReadS3(key=themeNamekey, bucket=bucketName)

    themeData['TargetFieldFunction'] = themeData['SourceFieldFunction']

    # Fix: Use .loc for all filtering. Create boolean masks.
    consFixedVals={}
    mask1 = (themeData["SourceFieldName"] != "") | themeData['TargetFieldFunction'].notna() | themeData['TargetFieldName'].isin(consFixedVals.keys())
    themeData = themeData.loc[mask1]

    mask2 = ~themeData['TargetFieldName'].isin(["InPrd From", "InPrd To", "Header",
                                                "PK", "SK", "Partition Key", "Sort Key",
                                                "InternalID", "Ver", "Version"])
    themeData = themeData.loc[mask2]

    themeData["ProcessCode"]=processCode
    themeData["ThemeName"]=themeName

    themeData = themeData.astype(object)
    themeData = themeData.where(pd.notna(themeData), None)
    themeData.replace(['NaN', 'None', 'nan', ''], None, inplace=True)

    return themeData

def ProcessDefn(productCode,processCode,tenantID,themeName=None):

    "Retrieves Process Definition From S3"

    query = "select * from process where process_cd <> 'process_cd' and process_cd = '%s'"%(processCode)

    # Invoice Presence In Database

    df_process=SQLRun(tenantID=tenantID,queryFile=None,writeTo="AthenaResults/",mode="Append",
                      writeFormat=".csv",returnResp=True,qText=query)

    df_process.rename(columns={
            'short_text': 'ShortText',
            'domain'    : 'Domain',
            'name'     : 'FieldName',
            'data_type' : 'DataType',
            'predefined_list': 'PredefinedList'
        }, inplace=True)
    
    if themeName is not None:

        df_theme = ThemeDataV2(productCode,tenantID,processCode,themeName)

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

def validate_column_format(inputdata, themedata, exceptions):
    for index, row in themedata.iterrows():
        source_field = row['SourceFieldName']
        data_type = row['TargetFieldDataType']
        raw_date_format = row.get('DataFormat')  # Expected format for DATE/DATETIME
        date_format = convert_to_strptime_format(raw_date_format) if raw_date_format else None

        if source_field not in inputdata.columns:
            continue  # Skip if source field not present in input data

        column_values = inputdata[source_field]

        if data_type in ['FLOAT', 'INTEGER']:
            # Normalize only string values
            column_values = column_values.apply(
                lambda x: str(x).strip().lower() if isinstance(x, str) else x
            )

            # Replace null-like values with np.nan
            column_values = column_values.replace(
                ['nan', 'none', 'pd.na', ''], np.nan
            )

            # Convert to numeric (invalid values become NaN)
            converted = pd.to_numeric(column_values, errors='coerce')

            # Identify non-numeric values (i.e., failed conversion and not true nulls)
            invalid_mask = converted.isna() & ~column_values.isin([np.nan])

            if invalid_mask.any():
                invalid_values = column_values[invalid_mask].unique().tolist()
                exceptions.append(
                    f"Column '{source_field}' contains non-numeric values for expected data type '{data_type}': {invalid_values}"
                )

        elif data_type in ['DATE', 'DATETIME']:
            datetime_format = f"{date_format} %H:%M:%S"
            if not date_format:
                exceptions.append(f"Expected date format is not specified for column '{source_field}'.")
                continue

            invalid_dates = []
            for val in column_values.dropna():
                str_val = str(val).strip()
                if str_val in ['2100-12-31', '1988-01-01', 'NaT', 'nan', 'NaN', 'NA', '<NA>', '']:
                    continue
                try:
                    datetime.strptime(str_val, date_format)
                except ValueError:
                    try:
                        datetime.strptime(str_val, datetime_format)
                    except ValueError:
                        invalid_dates.append(str_val)

            if invalid_dates:
                exceptions.append(
                    f"Column '{source_field}' must match date format '{raw_date_format}'. Found invalid values: {invalid_dates}"
                )

    return exceptions

def validate_source_field_function(themedata, inputdata,exceptions):

    # Check if 'Mandatory' column exists in themedata
    if 'Mandatory' not in themedata.columns:

        exceptions.append("The 'themedata' must contain 'Mandatory' column.")
    
    # Filter rows with "*" in 'Mandatory' column
    starred_rows = themedata[themedata['Mandatory'] == '*']
    
    # Initialize lists to store missing fields and missed Mandatory column names
    missing_fields = []
    missing_value_fields = []
    missed_source_field_functions = []

    # Iterate over starred rows
    for index, row in starred_rows.iterrows():

        source_field = row['SourceFieldName']

        if source_field not in inputdata.columns:

            missing_fields.append(source_field)

            missed_source_field_functions.append(row['Mandatory'])
        
        # Check if any value is missing in the mapped column
        elif inputdata[source_field].isnull().any():

            missing_value_fields.append(source_field)

            missed_source_field_functions.append(row['Mandatory'])

    # Construct error messages for missing columns and missing column values
    error_msgs = []

    if missing_fields:

        missing_fields_str = ', '.join(missing_fields)

        missed_source_field_functions_str = ', '.join(set(missed_source_field_functions))
        
        error_msgs.append(f"Missing Input Column: {missing_fields_str}")

    if missing_value_fields:

        missing_value_fields_str = ', '.join(missing_value_fields)

        missed_source_field_functions_str = ', '.join(set(missed_source_field_functions))
        
        error_msgs.append(f"Missing Input Column Values: {missing_value_fields_str}")

    # Raise error with constructed error messages
    if error_msgs:

        exceptions.append('\n'.join(error_msgs))
   
    return exceptions

def IntegrityCheck(tableName,processCode,themeName,inputdata,themedata,orgID,transactionType,transactionID):

    exceptions=[]

    # Validate Mandatory mapping
    exceptions = validate_source_field_function(themedata, inputdata, exceptions)

    exceptions = validate_column_format(inputdata, themedata, exceptions)

    if len(exceptions)>0:

        exceptions = ", ".join(exceptions)

        logger.info(f"Error: {exceptions} in {themeName}")

        DynamoLogger(txnID=str(pd.Timestamp.now().value),
                     process="PDT",
                     operation="PUT",
                     tableName=tableName,
                     attributes={"Stage":"IntegrityCheck",
                                 "OrgID":orgID,
                                 "TransactionID":transactionID,
                                 "TransactionType":transactionType,
                                 "ProcessCode":processCode,
                                 "ThemeName":themeName,
                                 "Error":exceptions
                                 })
        raise ("Integrity Check Issue: "+exceptions) 
    
    else:
        
        logger.info("Integrity Check Passed in "+themeName)
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

def SourceFieldNameToTargetFieldName(productCode, tenantID, processCode, themeName, input, orgID, transactionType, transactionID, Stage):

    exceptions=[]
    
    bucketName = 'dt-' + tenantID.lower() + suffixS3.lower()
    tableName = tenantID+suffixDb

    themedata = ThemeDataV2(productCode,tenantID,processCode,themeName)

    processdata = ProcessDefn(productCode,processCode,tenantID,themeName)

    # Read Data
    if isinstance(input,pd.DataFrame):

        inputdata=input 

    else:          

        try:

            inputdata = ReadS3(key=input, bucket=bucketName)

        except Exception as e:    

            exceptions.append("Failed/File Content Not Matching With The Extension:"+input)
            raise Exception(exceptions)

    if Stage != "3":   

        IntegrityCheck(tableName,processCode,themeName,inputdata,themedata,orgID,transactionType,transactionID)
        
        inputdata = StandardProcessDataMap(inputdata, themedata, processdata, exceptions)   

    else:

        inputdata = StandardProcessDataMapV1(inputdata, themedata, processdata, exceptions) 

    inputdata = inputdata.replace('', None) 

    inputdata = fill_numeric_nulls_from_themedata(inputdata, themedata)   

    if isinstance(inputdata,pd.DataFrame) and Stage == "1" and processCode == "BT3I":
    
        inputdata['uuid'] = [str(uuid.uuid4()) for _ in range(len(inputdata))]

        return inputdata
    
    else : 

        return inputdata
    
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

def get_ai_model(bucket,api_key,project_id,location,model_name,system_instruction):

    key = "VertexAI/credential.json"
    response=s3.get_object(Bucket=bucket,Key=key)
    raw_json = response["Body"].read().decode("utf-8")
    sa_info = json.loads(raw_json)
    credentials = service_account.Credentials.from_service_account_info(sa_info)

    vertexai.init(
    project=project_id,
    location=location,
    api_key=api_key,
    credentials=credentials
    )
    model = GenerativeModel(model_name,
                                
        system_instruction=[system_instruction])
    
    return model

def calling_aiV2(model, content):

    response = model.generate_content(content)

    response = response.text  

    return response

def transaction_identifier(bucket,DataKey,api_key,project_id,location,model_name):

    promptKey = "Prompt/Vertex ai/classification.txt"

    system_instruction = "You are an Advanced Financial Controller specialized in Intelligent Document Processing (IDP). You process financial data with 100% precision, mapping input to the correct database schema."

    model = get_ai_model(bucket,api_key,project_id,location,model_name,system_instruction)

    data = ReadS3(key=DataKey,bucket=bucket)

    prompt_data = ReadS3(key=promptKey,bucket=bucket)

    content = f"""
        {prompt_data}

        ### Input Data (CSV):
        {data}
        """

    response = calling_aiV2(model, content)

    cleaned = response.strip('`')          # removes backticks
    cleaned = cleaned.replace('json\n', '')  # removes 'json' label

    data = json.loads(cleaned)

    return data

def updating_response_in_db(ProductName,TenantID,OrgID,DataKey,TransactionType,TransactionID,ProcessCode,ThemeName):
    
    res = SourceFieldNameToTargetFieldName(ProductName, TenantID, ProcessCode, ThemeName, DataKey, OrgID, TransactionType, TransactionID, Stage='1')
    
    

def Job_Handler():

    bucket='dt-'+TenantID.lower()+suffixS3.lower()
    
    api_key = "AIzaSyCS3vVTFR26djNgUlyxuP6FdQLGqaOwFHo"
    project_id = "datatwinai-dev"
    location = "us-central1"
    model_name = "gemini-2.0-flash-001"
    
    #1.Logging Function Call
    
    #2.Identifying Transaction type using AI
    #response1 = transaction_identifier(bucket,DataKey,api_key,project_id,location,model_name)
    
    #3.
    
    #4.
    
    TransactionType = 'PurchaseOrder'
    ProcessCode = 'BT3I'
    ThemeName = 'POV1'
        
    response2 = updating_response_in_db(ProductName,TenantID,OrgID,DataKey,TransactionType,TransactionID,ProcessCode,ThemeName)



Process = "Data Modelling & Ingestion"
ProductName = "DTFOWS",
TenantID = "TID00002"
OrgID = "OM0000000002"
DataKey = "pretransaction/purchase-order-data.xlsx"
TransactionID = "TRONDI00226",
UserID = "dilip@datatwin.ai"

def main():
    
    Job_Handler()
