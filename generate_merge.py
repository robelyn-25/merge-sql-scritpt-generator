import pandas as pd
from datetime import datetime

# === Load Excel ===
df = pd.read_excel("[Name of excel file]].xlsx")

# === Column mapping ===
df.rename(columns={
    "Product Code": "ProductNumber",
    "Product Description": "ProductDescription",
    "Department": "Dept",
    "Weighted Cost": "UnitCost"
}, inplace=True)

# === Constant values used in all MERGE scripts ===
today = datetime.now().strftime("%m/%d/%Y")

# === Function to produce MERGE script for each row ===
def generate_merge(row):
    return f"""
MERGE INTO tblDeptProduct t
USING (
    SELECT 
        '{row.ProductNumber}' AS ProductNumber,
        '{row.Dept}' AS Dept,
        0 AS RSTLVL,
        0 AS PSPRICE,
        0 AS TYPE,
        '' AS GRP,
        '' AS DEPT,
        '{row.ProductNumberDESCR}' AS ProductNumberDESCR,
        ' ' AS STKSIZE
    FROM dual
) s
ON (t.ProductNumber = s.ProductNumber AND t.Dept = s.Dept)
WHEN MATCHED THEN 
    UPDATE SET
        -- update all fields except PK
        t.RSTLVL = s.RSTLVL,
        t.PSPRICE = s.PSPRICE,
        t."TYPE" = s."TYPE",
        t.GRP = s.GRP,
        t.DEPT = s.DEPT,
        t.ProductNumberDESCR = s.ProductNumberDESCR,
        t.STKSIZE = s.STKSIZE
WHEN NOT MATCHED THEN
    INSERT (
        ProductNumber, Dept, RSTLVL, PSPRICE,"TYPE", GRP, DEPT, ProductNumberDESCR,
        STKSIZE
    ) VALUES (
        s.ProductNumber, s.Dept, s.RSTLVL, s.PSPRICE,s."TYPE", s.GRP, s.DEPT,
        s.ProductNumberDESCR, s.STKSIZE
    );
"""

# === Generate all scripts ===
sql_output = "\n".join(df.apply(generate_merge, axis=1))

# === Save to file ===
with open("MERGE_OUTPUT.sql", "w", encoding="utf-8") as f:
    f.write(sql_output)

print("MERGE scripts generated → MERGE_OUTPUT.sql")
