import os
import docx
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import nsdecls, qn

def set_cell_background(cell, fill_hex):
    tcPr = cell._tc.get_or_add_tcPr()
    shd = parse_xml(f'<w:shd {nsdecls("w")} w:fill="{fill_hex}"/>')
    tcPr.append(shd)

def set_cell_margins(cell, top=100, bottom=100, left=150, right=150):
    tcPr = cell._tc.get_or_add_tcPr()
    tcMar = OxmlElement('w:tcMar')
    for m, val in [('top', top), ('bottom', bottom), ('left', left), ('right', right)]:
        node = OxmlElement(f'w:{m}')
        node.set(qn('w:w'), str(val))
        node.set(qn('w:type'), 'dxa')
        tcMar.append(node)
    tcPr.append(tcMar)

def add_callout(doc, text, title="NOTE"):
    tbl = doc.add_table(rows=1, cols=1)
    tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
    cell = tbl.cell(0, 0)
    set_cell_background(cell, "F0F4F8")
    set_cell_margins(cell, top=140, bottom=140, left=200, right=200)
    
    # Left border styling
    tcPr = cell._tc.get_or_add_tcPr()
    borders = parse_xml(
        f'<w:tcBorders {nsdecls("w")}>'
        f'<w:top w:val="none"/>'
        f'<w:left w:val="single" w:sz="24" w:space="0" w:color="0052CC"/>'
        f'<w:bottom w:val="none"/>'
        f'<w:right w:val="none"/>'
        f'</w:tcBorders>'
    )
    tcPr.append(borders)
    
    p = cell.paragraphs[0]
    p.paragraph_format.space_before = Pt(2)
    p.paragraph_format.space_after = Pt(2)
    run_title = p.add_run(f"[{title}] ")
    run_title.bold = True
    run_title.font.name = "Calibri"
    run_title.font.color.rgb = RGBColor(0, 82, 204)
    run_title.font.size = Pt(10.5)
    
    run_text = p.add_run(text)
    run_text.font.name = "Calibri"
    run_text.font.size = Pt(10.5)
    run_text.font.color.rgb = RGBColor(33, 37, 41)
    
    doc.add_paragraph().paragraph_format.space_after = Pt(4)

def format_table(table, col_widths, headers, data):
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    hdr_cells = table.rows[0].cells
    for i, title in enumerate(headers):
        hdr_cells[i].text = title
        set_cell_background(hdr_cells[i], "1F497D")
        set_cell_margins(hdr_cells[i], top=120, bottom=120, left=140, right=140)
        p = hdr_cells[i].paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.LEFT
        for r in p.runs:
            r.font.bold = True
            r.font.name = "Calibri"
            r.font.color.rgb = RGBColor(255, 255, 255)
            r.font.size = Pt(10)
            
    for row_idx, row_data in enumerate(data):
        row_cells = table.rows[row_idx + 1].cells
        bg_color = "F9FAFB" if row_idx % 2 == 1 else "FFFFFF"
        for col_idx, cell_value in enumerate(row_data):
            row_cells[col_idx].text = str(cell_value)
            set_cell_background(row_cells[col_idx], bg_color)
            set_cell_margins(row_cells[col_idx], top=80, bottom=80, left=140, right=140)
            p = row_cells[col_idx].paragraphs[0]
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            for r in p.runs:
                r.font.name = "Calibri"
                r.font.size = Pt(9.5)
                r.font.color.rgb = RGBColor(33, 37, 41)

    for row in table.rows:
        for idx, width in enumerate(col_widths):
            row.cells[idx].width = width

def build_document(output_path):
    doc = Document()
    
    # Page setup - Margins
    sections = doc.sections
    for section in sections:
        section.top_margin = Inches(1.0)
        section.bottom_margin = Inches(1.0)
        section.left_margin = Inches(1.0)
        section.right_margin = Inches(1.0)
        
    # Styles
    styles = doc.styles
    normal_style = styles['Normal']
    normal_style.font.name = 'Calibri'
    normal_style.font.size = Pt(11)
    normal_style.font.color.rgb = RGBColor(40, 44, 52)
    normal_style.paragraph_format.line_spacing = 1.15
    normal_style.paragraph_format.space_after = Pt(6)

    # Document Header / Cover Area
    p_title = doc.add_paragraph()
    p_title.paragraph_format.space_before = Pt(12)
    p_title.paragraph_format.space_after = Pt(4)
    run_title = p_title.add_run("DataTwin AI Ingestion Engine")
    run_title.font.name = "Calibri"
    run_title.font.size = Pt(26)
    run_title.font.bold = True
    run_title.font.color.rgb = RGBColor(31, 73, 125)

    p_sub = doc.add_paragraph()
    p_sub.paragraph_format.space_after = Pt(16)
    run_sub = p_sub.add_run("Technical Architecture, AI Orchestration & Database Specification (app.py)")
    run_sub.font.name = "Calibri"
    run_sub.font.size = Pt(14)
    run_sub.font.color.rgb = RGBColor(89, 89, 89)

    # Metadata Card
    meta_table = doc.add_table(rows=5, cols=2)
    meta_table.alignment = WD_TABLE_ALIGNMENT.CENTER
    meta_data = [
        ("Platform / System", "DataTwin AI (DTFOWS Platform)"),
        ("Script Role", "Autonomous IDP, Schema Generation & Database Ingestion (AWS Glue / Python CLI)"),
        ("AI Foundation", "Google Vertex AI (Gemini 2.5 Flash) via google-genai SDK"),
        ("Storage & Formats", "Amazon S3 (.xlsx, .xls, .xlsm, .csv, .tsv, .json)"),
        ("Database Target", "Multi-Tenant PostgreSQL (Dynamic Schemas, Active/Inactive Partitioning)")
    ]
    for idx, (k, v) in enumerate(meta_data):
        c0 = meta_table.cell(idx, 0)
        c1 = meta_table.cell(idx, 1)
        c0.text = k
        c1.text = v
        set_cell_background(c0, "F0F4F8")
        set_cell_background(c1, "FFFFFF")
        set_cell_margins(c0, top=70, bottom=70, left=120, right=120)
        set_cell_margins(c1, top=70, bottom=70, left=120, right=120)
        c0.paragraphs[0].runs[0].font.bold = True
        c0.paragraphs[0].runs[0].font.size = Pt(9.5)
        c0.paragraphs[0].runs[0].font.color.rgb = RGBColor(31, 73, 125)
        c1.paragraphs[0].runs[0].font.size = Pt(9.5)
        c0.width = Inches(2.2)
        c1.width = Inches(4.3)
        
    doc.add_paragraph().paragraph_format.space_after = Pt(12)

    # Section 1: Executive Summary
    h1 = doc.add_heading("1. Executive Summary & Purpose", level=1)
    h1.paragraph_format.space_before = Pt(14)
    h1.paragraph_format.space_after = Pt(6)
    
    doc.add_paragraph(
        "The DataTwin Ingestion Engine (app.py) is an autonomous data ingestion service that eliminates manual "
        "ETL development. It accepts arbitrary financial files (Vendor Bills, Invoices, Contracts, Goods Receipts, GSTR filings), "
        "uses Google Vertex AI (Gemini 2.5 Flash) to classify the transaction type, coins standardized ERP schemas, creates PostgreSQL "
        "tables on the fly, and ingests multi-sheet spreadsheets with full audit traceability."
    )

    # Section 2: CLI & Execution Arguments
    h2 = doc.add_heading("2. Execution Arguments & CLI Reference", level=1)
    h2.paragraph_format.space_before = Pt(14)
    h2.paragraph_format.space_after = Pt(6)

    doc.add_paragraph(
        "app.py can be invoked either via standard command-line flags or through AWS Glue job arguments. "
        "The table below details every parameter accepted by the ingestion pipeline:"
    )

    t_params = doc.add_table(rows=12, cols=3)
    param_widths = [Inches(1.8), Inches(1.8), Inches(2.9)]
    param_headers = ["Argument", "Type / Example", "Description & Usage"]
    param_data = [
        ("--Process", "String: 'Data Modelling & Ingestion'", "Execution workflow: 'Data Modelling & Ingestion', 'Data Ingestion', or 'Super Admin Correction'."),
        ("--ProductName", "String: 'DTFOWS'", "Platform product code identifier."),
        ("--TenantID", "String: 'DM00005'", "Tenant ID. Determines the target PostgreSQL database (dm00005) and S3 bucket (dt-dm00005-dev)."),
        ("--OrgID", "String: 'OM0000000002'", "Client organizational unit identifier within the tenant."),
        ("--DataKey", "String: 'pretransaction/bill.xlsx'", "S3 key pointing to the source file within the tenant S3 bucket."),
        ("--TransactionID", "String: 'TROMDI00006'", "Tracking session ID representing this ingestion job."),
        ("--UserID", "String: 'dilip@datatwin.ai'", "Email or ID of operator/service user (saved to createdby/modifiedby)."),
        ("--TransactionType", "String: 'Vendor Bill'", "Document business type (used in direct 'Data Ingestion' mode)."),
        ("--ProcessCode", "String: 'BT3I'", "Target process code (used in direct 'Data Ingestion' mode)."),
        ("--ThemeName", "String: 'THEME0000001'", "Mapping theme name (used in direct 'Data Ingestion' mode)."),
        ("--OldChanges / --NewChanges", "JSON String / Dict", "Old and new mapping configurations used during 'Super Admin Correction'.")
    ]
    format_table(t_params, param_widths, param_headers, param_data)

    add_callout(
        doc,
        "Notice for Local Execution: Lines 2122-2128 of app.py define local test variables (Process, TenantID, DataKey, etc.). "
        "When running with dynamic CLI arguments, ensure these lines are commented out or modified to avoid overriding CLI inputs.",
        "IMPORTANT NOTICE"
    )

    # Section 3: How AI is Called
    h3 = doc.add_heading("3. How AI is Called (Vertex AI & Gemini Integration)", level=1)
    h3.paragraph_format.space_before = Pt(14)
    h3.paragraph_format.space_after = Pt(6)

    doc.add_paragraph(
        "AI calls in app.py use the modern google-genai SDK connecting to Google Vertex AI. The architecture includes:\n"
        "• Credentials Loading (get_ai_client): Service account JSON is retrieved dynamically from the tenant's S3 bucket at "
        "'VertexAI/credential.json' and scoped with 'https://www.googleapis.com/auth/cloud-platform'.\n"
        "• AI Client: genai.Client(vertexai=True, project='datatwinai-dev', location='us-central1', credentials=scoped_credentials).\n"
        "• Model: gemini-2.5-flash with types.GenerateContentConfig(system_instruction=...).\n"
        "• Output Parsing: Markdown wrappers (```json) are stripped and cleaned before JSON parsing."
    )

    # Section 4: Inputs Provided to AI
    h4 = doc.add_heading("4. Inputs Provided to AI (Prompts & Data)", level=1)
    h4.paragraph_format.space_before = Pt(14)
    h4.paragraph_format.space_after = Pt(6)

    t_ai = doc.add_table(rows=4, cols=3)
    ai_widths = [Inches(1.8), Inches(2.2), Inches(2.5)]
    ai_headers = ["Stage", "Inputs Provided", "Expected AI Output"]
    ai_data = [
        ("Stage 1: Classification\n(transaction_identifier)", 
         "1. Prompt: 'Prompt/Vertex ai/classification.txt'\n2. System Instruction (Financial Controller)\n3. All rows from DataTwin_DB.processcodes_knowledge\n4. First 3 rows of file (df.head(3).to_string())", 
         "JSON:\n{\n  'TransactionType': 'Vendor Bill',\n  'ProcessCode': 'BT3I' (or 'New')\n}"),
        
        ("Stage 2A: New Process\n(handle_new_process_metadata)", 
         "1. Prompt: 'Prompt/Vertex ai/ERP_Generator.txt'\n2. Existing process_cd list from DataTwin_DB.process\n3. TransactionType from Stage 1\n4. First 3 rows of file", 
         "JSON:\n{\n  'Metadata': [ProcessCode, ShortText, DataType, LDE, Indexing, SourceFieldName],\n  'ProcessCodeKnowledge': {...}\n}"),
         
        ("Stage 2B: Existing Process\n(handle_existing_process_metadata)", 
         "1. Prompt: 'Prompt/Vertex ai/prompt.EP.txt'\n2. Existing target fields from DataTwin_DB.process\n3. Existing themes from theme_mapping\n4. First 3 rows of file", 
         "JSON Array:\n[\n  {'ProcessCode': '...', 'ThemeName': '...', 'TargetFieldName': '...', 'SourceFieldName': '...'}\n]")
    ]
    format_table(t_ai, ai_widths, ai_headers, ai_data)

    # Section 5: Available Tables
    h5 = doc.add_heading("5. Database Architecture & Available Tables", level=1)
    h5.paragraph_format.space_before = Pt(14)
    h5.paragraph_format.space_after = Pt(6)

    doc.add_paragraph(
        "DataTwin implements a multi-tenant PostgreSQL structure separated into CommonDB (DataTwin_DB) and TenantDB:"
    )

    t_tables = doc.add_table(rows=7, cols=3)
    tbl_widths = [Inches(1.8), Inches(1.8), Inches(2.9)]
    tbl_headers = ["Table Name", "Database Scope", "Role & Key Columns"]
    tbl_data = [
        ("process", "CommonDB (DataTwin_DB)", "Master ERP catalog of process codes, target fields (short_text/domain), data types, and LDE rankings."),
        ("processcodes_knowledge", "CommonDB (DataTwin_DB)", "Semantic knowledge base describing each process code for zero-shot LLM classification."),
        ("masterobjectsequencing", "CommonDB & TenantDB", "Atomic monotonic counter issuing collision-free IDs (TXN0000001, etc.)."),
        ("theme_mapping", "TenantDB (dm<tenantid>)", "Translation dictionary mapping vendor source columns to standardized target field names."),
        ("active_<processcode>", "TenantDB (dm<tenantid>)", "Physical storage of active records. Composite PK: (processcode, themename, LDEs, sno). Includes unmapped_fields JSONB."),
        ("inactive_<processcode>", "TenantDB (dm<tenantid>)", "Historical versions preserved during Super Admin Corrections. PK includes inprdfrom for audit validity.")
    ]
    format_table(t_tables, tbl_widths, tbl_headers, tbl_data)

    # Section 6: Existing Table vs New Table Insertion
    h6 = doc.add_heading("6. Inserting into Existing Table vs Creating New Table", level=1)
    h6.paragraph_format.space_before = Pt(14)
    h6.paragraph_format.space_after = Pt(6)

    doc.add_paragraph(
        "A. Creating and Inserting into a NEW Table (ProcessCode == 'New'):\n"
        "1. AI synthesizes a new unique ProcessCode (e.g. GSTR2B), column definitions, data types, and LDE (Logical Data Entity) rankings.\n"
        "2. Metadata is inserted into DataTwin_DB.process, theme_mapping, and processcodes_knowledge.\n"
        "3. createtableinPostgres() executes DDL creating active_<processcode> and inactive_<processcode>. "
        "The primary key is composed of (processcode, themename, sorted LDE fields, sno).\n"
        "4. updating_response_in_db() runs data transformations and executes bulk UPSERT.\n\n"
        "B. Inserting into an EXISTING Table (ProcessCode != 'New'):\n"
        "1. AI maps the incoming vendor headers to the known target fields of the existing process.\n"
        "2. A new theme row is registered in theme_mapping.\n"
        "3. Incoming data is mapped to target columns, validated, enriched with audit timestamps, and bulk UPSERTed into active_<processcode>."
    )

    # Section 7: Finding and Creating Column Names
    h7 = doc.add_heading("7. Column Name Discovery & Creation", level=1)
    h7.paragraph_format.space_before = Pt(14)
    h7.paragraph_format.space_after = Pt(6)

    doc.add_paragraph(
        "How Column Names are Found:\n"
        "• S3 readers (ReadS3 / ReadAllSheetsS3) extract raw header rows from Excel, CSV, TSV, or JSON.\n"
        "• Headers are normalized to lowercase and whitespace-stripped.\n"
        "• Semantic LLM matching aligns vendor-specific column variations (e.g. 'Inv_No', 'Bill#', 'DocNum') to canonical target domains (e.g. 'InvoiceNumber').\n\n"
        "How New Column Names are Created:\n"
        "• For new processes, Gemini coins standardized ERP domain names (PascalCase/CamelCase) along with an LDE (Logical Data Entity) level.\n"
        "• Columns with LDE > 0 are designated as primary identification keys.\n"
        "• Unmapped / Bespoke Columns (Zero Data Loss): Any vendor column not mapped in theme_mapping is automatically packed into "
        "a JSON object and stored in the dedicated 'unmapped_fields' (JSONB) column on PostgreSQL, ensuring zero data loss."
    )

    doc.save(output_path)
    print(f"Successfully generated complete Word document at: {output_path}")

if __name__ == "__main__":
    out_file = r"c:\VR3 PROJECT\injection\DataTwin_Injection_Engine_Documentation.docx"
    build_document(out_file)
