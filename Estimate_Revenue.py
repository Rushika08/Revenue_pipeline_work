import os
import re
import pandas as pd
from sqlalchemy import create_engine
import urllib
from dotenv import load_dotenv

# -------------------------------------------------------------
# 0. CONFIG
# -------------------------------------------------------------
FOLDER_PATH = r"E:\Revenue Sources\Estimate Revenue"
EXCEL_EXTENSION = ".xlsx"

# -------------------------------------------------------------
# 1. SQL SERVER CONNECTION (SECURE)
# -------------------------------------------------------------
load_dotenv()

server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

if not all([server, database, username, password]):
    raise ValueError("❌ Database environment variables not loaded")

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
# 2. LOOP THROUGH ALL EXCEL FILES
# -------------------------------------------------------------
excel_files = [
    f for f in os.listdir(FOLDER_PATH)
    if f.lower().endswith(EXCEL_EXTENSION)
]

if not excel_files:
    raise ValueError("❌ No Excel files found in folder.")

for file_name in excel_files:
    file_path = os.path.join(FOLDER_PATH, file_name)
    print(f"\n📄 Processing file: {file_name}")

    try:
        # -----------------------------------------------------
        # 3. READ EXCEL (4th row header)
        # -----------------------------------------------------
        df = pd.read_excel(file_path, header=3, usecols="A,B,D:O")
        df.columns = df.columns.str.strip()

        # -----------------------------------------------------
        # 4. FILTER VALID REVENUE CODES
        # -----------------------------------------------------
        pattern = re.compile(r"^\d{4}\.\d{2}\.\d{2}$")
        revenue_code_col = df.columns[0]

        df = df[df[revenue_code_col].astype(str).str.match(pattern, na=False)]
        df.reset_index(drop=True, inplace=True)

        # -----------------------------------------------------
        # 5. CLEAN VALUES
        # -----------------------------------------------------
        df = df.replace("-", pd.NA)
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        df.iloc[:, 1] = df.iloc[:, 1].fillna(df.iloc[:, 0])

        # -----------------------------------------------------
        # 6. MELT MONTHS
        # -----------------------------------------------------
        fixed_cols = df.columns[:2]
        month_cols = df.columns[2:]

        df_melted = df.melt(
            id_vars=fixed_cols,
            value_vars=month_cols,
            var_name="Month",
            value_name="Value"
        )

        # -----------------------------------------------------
        # 7. EXTRACT YEAR FROM FILE NAME
        # -----------------------------------------------------
        match = re.search(r"(\d{4})", file_name)
        if not match:
            raise ValueError("No year found in filename")

        extracted_year = int(match.group(1))
        df_melted["Year"] = extracted_year

        # -----------------------------------------------------
        # 8. FINAL STRUCTURE
        # -----------------------------------------------------
        df_final = df_melted.rename(columns={
            df.columns[0]: "Revenue_Code",
            df.columns[1]: "Revenue_Source"
        })[["Year", "Month", "Revenue_Code", "Revenue_Source", "Value"]]

        df_final["Value"] = pd.to_numeric(df_final["Value"], errors="coerce")

        # -----------------------------------------------------
        # 9. LOAD INTO SQL
        # -----------------------------------------------------
        df_final.to_sql(
            name="Estimate_Revenue",
            con=engine,
            schema="InsightStaging",
            if_exists="append",
            index=False,
            method="multi"
        )

        print(f"✅ Loaded {len(df_final)} rows from {file_name}")

    except Exception as e:
        print(f"❌ Failed processing {file_name}: {e}")

print("\n🎉 All files processed.")
