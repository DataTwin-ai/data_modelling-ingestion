import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

data = [{
    "BUKRS": "2024",
    "KUNNR": "CUST02",
    "UMSKS": "",
    "UMSKZ": "",
    "AUGDT": "0000-00-00",
    "AUGBL": "",
    "ZUONR": "MMI31300098",
    "GJAHR": "2026",
    "BELNR": "0100000152",
    "BUZEI": "001",
    "BUDAT": "2026-05-06",
    "BLDAT": "2026-05-06",
    "WAERS": "EUR",
    "DMBTR": 5000.0,
    "WRBTR": 5000.0,
}]
df = pd.DataFrame(data)
path = "BSID_ITEMS_converted.xlsx"
df.to_excel(path, index=False, sheet_name="BSID_ITEMS")

wb = load_workbook(path)
ws = wb["BSID_ITEMS"]
ws.freeze_panes = "A2"
ws.auto_filter.ref = ws.dimensions
for cell in ws[1]:
    cell.font = Font(bold=True, color="FFFFFF")
    cell.fill = PatternFill("solid", fgColor="1F4E78")
for col_idx, col_name in enumerate(df.columns, start=1):
    max_len = max(len(str(col_name)), max(len(str(v)) for v in df[col_name].fillna("").tolist())) + 2
    ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len, 24)
# Preserve SAP identifiers and dates as text, especially leading zeros and zero date
for row in ws.iter_rows(min_row=2):
    for idx in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]:
        row[idx-1].number_format = "@"
wb.save(path)
print(path)
