import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text

# Database connection details
DB_HOST = "localhost"
DB_NAME = "Shakedeal"
DB_USER = "postgres"
DB_PASSWORD = "2705"

# Target Table Name
TABLE_NAME = "FBA_Inventory"

# Folder path where CSV files are stored
BASE_FOLDER = r"C:\Users\Ranjith\OneDrive - MSFT\Shakedeal Documents\Report Files\FC_Inventory\Amazon"

# Establish database connection with client encoding set to UTF8
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}",
    connect_args={"client_encoding": "utf8"}
)

def infer_sqlalchemy_dtype(dtype):
    """Convert pandas dtype to SQL-compatible type."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"  # Default to TEXT for strings

def create_fba_inventory_table(df):
    """Dynamically create the 'FBA_Inventory' table based on the CSV file structure."""
    columns_with_types = []
    
    for column in df.columns:
        column_cleaned = column.strip().replace(" ", "_").lower()
        sql_type = infer_sqlalchemy_dtype(df[column])
        columns_with_types.append(f'"{column_cleaned}" {sql_type}')
    
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" (
            id SERIAL PRIMARY KEY,
            {", ".join(columns_with_types)}
        );
    """
    
    with engine.begin() as conn:
        conn.execute(text(create_table_query))
    
    print(f"✅ Table `{TABLE_NAME}` created dynamically.")

def find_csv_files(folder_path):
    """Find all CSV files in the given folder."""
    csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".csv")]
    return csv_files

def detect_encoding(file_path):
    """Detects the encoding of the CSV file."""
    import chardet
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read(100000))  # Read first 100,000 bytes
    return result['encoding']

def clean_non_utf8(df):
    """Convert all string columns to UTF-8 and remove invalid characters."""
    for col in df.select_dtypes(include=[object]):
        df[col] = df[col].astype(str).apply(lambda x: x.encode('utf-8', 'ignore').decode('utf-8'))
    return df

def load_data_into_db(file_path):
    """Load data from CSV into the `FBA_Inventory` table while handling encoding issues."""
    print(f"🔹 Processing file: {file_path}")

    try:
        # Detect file encoding
        detected_encoding = detect_encoding(file_path)
        print(f"🧐 Detected encoding: {detected_encoding}")

        # Read CSV file with detected encoding
        df = pd.read_csv(file_path, encoding=detected_encoding, encoding_errors='replace')

        # If the file is empty, skip processing
        if df.empty:
            print(f"⚠️ Warning: {file_path} is empty. Skipping file.")
            return
        
        # Normalize column names
        df.rename(columns=lambda x: x.strip().replace(" ", "_").lower(), inplace=True)

        # Clean non-UTF-8 characters
        df = clean_non_utf8(df)

        # Print Data Preview (Debugging Step)
        print("\n🔍 Preview of Data to be Inserted:\n", df.head())

        # Ensure table is created dynamically based on CSV file structure
        create_fba_inventory_table(df)  

        # Insert data into database using a transactional context
        with engine.begin() as conn:
            df.to_sql(TABLE_NAME, con=conn, if_exists='append', index=False, method="multi")

        print(f"✅ Data uploaded successfully from {file_path} into `{TABLE_NAME}`")

    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")

def verify_data_in_db():
    """Check if data is inserted into the database."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM \"{TABLE_NAME}\""))
        count = result.scalar()
        print(f"📊 Rows in `{TABLE_NAME}`: {count}")

def clear_table():
    """
    Truncate the existing table if it exists to remove old data before loading new data.
    If the table doesn't exist, an error is caught and the table will be created later.
    """
    with engine.begin() as conn:
        try:
            conn.execute(text(f'TRUNCATE TABLE "{TABLE_NAME}"'))
            print(f"✅ Table `{TABLE_NAME}` truncated.")
        except Exception as e:
            print(f"⚠️ Could not truncate table `{TABLE_NAME}` (it might not exist yet). It will be created later. Error: {e}")

def main():
    csv_files = find_csv_files(BASE_FOLDER)
    if not csv_files:
        print("⚠️ No CSV files found in the folder.")
        return

    # Clear old data using TRUNCATE instead of DROP
    clear_table()

    for file in csv_files:
        load_data_into_db(file)

    verify_data_in_db()  # Check if data is inserted

if __name__ == "__main__":
    main()
