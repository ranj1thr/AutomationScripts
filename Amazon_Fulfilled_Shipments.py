import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.types import Date
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

# === Step 1: Folder & DB Config ===
folder_path = r'C:\Users\Ranjith\OneDrive - MSFT\Shakedeal Documents\Report Files\Amazon\FBA_Orders'

host = 'localhost'
database = 'Shakedeal'
port = '5432'
user = 'postgres'
password = '2705'
table_name = 'Amazon_Fulfilled_Shipments'

# === Step 2: Walk through all subfolders and collect CSV files ===
csv_files = []
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith('.csv'):
            csv_files.append(os.path.join(root, file))

# === Step 3: Display files for user selection ===
print("\nAvailable CSV Files:")
for i, f in enumerate(csv_files):
    print(f"[{i}] {f}")

user_input = input("\nEnter the file numbers you want to process (comma-separated): ").split(',')

# === Step 4: Collect selected file paths ===
selected_files = []
for index in user_input:
    index = index.strip()
    if index.isdigit() and int(index) < len(csv_files):
        selected_files.append(csv_files[int(index)])

# === Step 5: Read & Combine DataFrames ===
df_list = [pd.read_csv(f) for f in selected_files]
df = pd.concat(df_list, ignore_index=True)

# === Step 6: Convert date columns to datetime.date ===
date_columns = ['Payments Date', 'Purchase Date', 'Shipment Date', 'Reporting Date']
for col in date_columns:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

# === Step 7: Create a Unique Key ===
df['unique_key'] = df['Amazon Order Id'].astype(str) + '-' + \
                   df['Shipment ID'].astype(str) + '-' + \
                   df['Shipment Item Id'].astype(str)

# === Step 8: Create DB connection ===
connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
engine = create_engine(connection_string)

# === Step 9: Check and remove already uploaded rows ===
try:
    query = f'SELECT "unique_key" FROM "{table_name}";'
    existing_keys_df = pd.read_sql(query, engine)
    existing_keys = set(existing_keys_df['unique_key'].dropna().unique())
    before_filter = df.shape[0]
    df = df[~df['unique_key'].isin(existing_keys)]
    duplicates_filtered = before_filter - df.shape[0]
except Exception as e:
    print("⚠️ Could not fetch existing keys, assuming fresh table:", e)
    duplicates_filtered = 0

# === Step 10: Set SQL data types for date columns ===
sql_dtypes = {col: Date() for col in date_columns if col in df.columns}

# === Step 11: Upload filtered data ===
if not df.empty:
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False, dtype=sql_dtypes)
        print(f"\n✅ Uploaded {df.shape[0]} new rows to '{table_name}'")
        print(f"🚫 Skipped {duplicates_filtered} duplicate rows based on 'unique_key'")
        print(df[['Amazon Order Id', 'Shipment ID', 'Shipment Item Id', 'unique_key']].head())
    except Exception as e:
        print(f"\n❌ Error uploading to database: {e}")
else:
    print("\n🚫 No new records to upload (all were duplicates).")
