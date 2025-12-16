import os
import re
import pandas as pd
from sqlalchemy import create_engine
import urllib
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()

# -------------------------------------------------------------
# 1. SQL SERVER CONNECTION
# -------------------------------------------------------------
# 🔐 Read credentials from .env
server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

# 🚨 Safety check
if not all([server, database, username, password]):
    raise ValueError("❌ Database environment variables not loaded")

# ✅ WORKING SQLALCHEMY CONNECTION (odbc_connect)
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
    "Encrypt=no;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
print("✅ Connection established with SQL Server.")

# -------------------------------------------------------------
# 2. READ EXCEL WITH 4TH ROW AS HEADER
# -------------------------------------------------------------
file_path = r'C:\Users\rushika\Downloads\Actual revenue ETL\2020 Actual Revenue.xlsx'

df = pd.read_excel(file_path, header=3, usecols="A:N")
df.columns = df.columns.str.strip()

# -------------------------------------------------------------
# 3. IDENTIFY VALID REVENUE CODE ROWS (xxxx.xx.xx)
# -------------------------------------------------------------
pattern = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
valid_rows = df[df.iloc[:, 0].astype(str).str.match(pattern, na=False)]

# Slice only up to the last valid row
last_index = valid_rows.index[-1]
df = df.loc[:last_index].copy()

# -------------------------------------------------------------
# 4. CLEAN VALUES
# -------------------------------------------------------------
df = df.replace("-", pd.NA)
df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

# If Revenue Source is blank → fill using Revenue Code
df.iloc[:, 1] = df.iloc[:, 1].fillna(df.iloc[:, 0])

# -------------------------------------------------------------
# 5. SPLIT FIXED & MONTHLY COLUMNS
# -------------------------------------------------------------
fixed_cols = df.columns[:2]      # Revenue Code, Revenue Source
month_cols = df.columns[2:]      # Jan–Dec columns

# -------------------------------------------------------------
# 6. MELT TO LONG FORMAT
# -------------------------------------------------------------
df_melted = df.melt(
    id_vars=fixed_cols,
    value_vars=month_cols,
    var_name="Month",
    value_name="Value"
)

# -------------------------------------------------------------
# 7. ADD YEAR AND DATE FROM FILE NAME
# -------------------------------------------------------------
file_name = os.path.basename(file_path)
match = re.search(r"(\d{4})", file_name)

if not match:
    raise ValueError("❌ No 4-digit year found in the file name.")

extracted_year = int(match.group(1))

df_melted["Year"] = extracted_year
df_melted["Date"] = df_melted["Year"].astype(str) + " " + df_melted["Month"].astype(str)

# -------------------------------------------------------------
# 8. FINAL RENAME
# -------------------------------------------------------------
df_final = df_melted.rename(columns={
    df.columns[0]: "Revenue Code",
    df.columns[1]: "Revenue Source"
})[["Date", "Revenue Code", "Revenue Source", "Value"]]

# -------------------------------------------------------------
# 9. ENSURE ALL 12 MONTHS EXIST
# -------------------------------------------------------------
month_order = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

unique_codes = df_final["Revenue Code"].unique()

reindex_template = pd.MultiIndex.from_product(
    [unique_codes, month_order],
    names=["Revenue Code", "Month"]
)

df_final["Month"] = df_final["Date"].str.split().str[1]

df_final = (
    df_final
    .set_index(["Revenue Code", "Month"])
    .reindex(reindex_template)
    .reset_index()
)

df_final["Year"] = extracted_year

df_final = df_final[["Year", "Month", "Revenue Code", "Revenue Source", "Value"]]

df_final = df_final.rename(columns={
    "Revenue Code": "Revenue_Code",
    "Revenue Source": "Revenue_Source"
})

# Numeric conversion
df_final["Value"] = pd.to_numeric(df_final["Value"], errors="coerce")

# -------------------------------------------------------------
# 10. PREVIEW
# -------------------------------------------------------------
print(df_final.head(20))
print(f"\nTotal rows after ensuring 12 months per Revenue Code: {len(df_final)}")

# -------------------------------------------------------------
# 11. LOAD INTO SQL
# -------------------------------------------------------------
table_name = "Actual_Revenue"
schema_name = "InsightStaging"

df_final.to_sql(
    table_name,
    con=engine,
    schema=schema_name,
    if_exists='append',
    index=False
)

print(f"✅ Successfully loaded {len(df_final)} rows into table '{schema_name}.{table_name}'.")
