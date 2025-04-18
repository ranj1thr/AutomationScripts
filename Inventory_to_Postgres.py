import pandas as pd
import os
from io import StringIO
from rich.progress import Progress
from rich.console import Console
from rich.table import Table
from psycopg2 import pool

# PostgreSQL Connection details
host = 'localhost'
database = 'Shakedeal'
user = 'postgres'
password = '2705'
table_name = 'Inventory'

# Inventory folder path address
folder_path = r'C:\Users\Ranjith\OneDrive - MSFT\Shakedeal Documents\Inventory'

# Create a PostgreSQL connection pool
connection_pool = pool.SimpleConnectionPool(1, 10, user=user, password=password, host=host, database=database)

# Function to get a database connection
def get_db_connection():
    return connection_pool.getconn()

# Function to release a database connection
def release_db_connection(conn):
    connection_pool.putconn(conn)

# Function to truncate the table (Only once at the start)
def truncate_table(table_name):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f'TRUNCATE TABLE "{table_name}"')
            conn.commit()
            print(f"Table '{table_name}' truncated successfully.")
    finally:
        release_db_connection(conn)

# Function to upload data to the database using bulk inserts
def upload_data_bulk(df, table_name):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cursor.copy_expert(f'COPY "{table_name}" FROM STDIN WITH CSV', buffer)
        conn.commit()
        print(f"Data appended successfully to table '{table_name}'.")
    finally:
        release_db_connection(conn)

# Function to create the table if it does not exist
def create_table_if_not_exists(df, table_name):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    {', '.join(f'"{col}" TEXT' for col in df.columns)}
                )
            """)
            conn.commit()
            print(f"Table '{table_name}' verified or created.")
    finally:
        release_db_connection(conn)

# Function to process and upload data from all subfolders
def process_and_upload_files(folder_path, table_name):
    console = Console()
    summary = []  # List to store summary information for tabular output

    # Collect all files to process
    files_to_process = []
    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            if file_name.endswith('.xlsx') or file_name.endswith('.csv'):
                files_to_process.append(os.path.join(root, file_name))

    total_files = len(files_to_process)
    console.print(f"Found {total_files} files to process.\n", style="bold green")

    if total_files == 0:
        console.print("No files found. Exiting process.", style="bold red")
        return

    # Ensure the table exists
    df_sample = pd.read_csv(files_to_process[0]) if files_to_process[0].endswith('.csv') else pd.read_excel(files_to_process[0])
    create_table_if_not_exists(df_sample, table_name)

    # TRUNCATE ONLY ONCE AT THE START
    truncate_table(table_name)

    with Progress() as progress:
        task = progress.add_task("[cyan]Processing Files...", total=total_files)

        for file_path in files_to_process:
            file_name = os.path.basename(file_path)

            # Load the file into a DataFrame
            try:
                if file_name.endswith('.xlsx'):
                    df = pd.read_excel(file_path)
                elif file_name.endswith('.csv'):
                    df = pd.read_csv(file_path)

                # Upload data (Append to the already truncated table)
                upload_data_bulk(df, table_name)

                summary.append({
                    "File Name": file_name,
                    "Rows": len(df),
                    "Status": f"✅ {len(df)} Rows Inserted Successfully"
                })

            except Exception as e:
                summary.append({
                    "File Name": file_name,
                    "Rows": "N/A",
                    "Status": f"❌ Error: {str(e)}"
                })

            progress.update(task, advance=1)  # Update progress bar

    # Create table for summary
    table = Table(title="Summary of Processed Files")
    table.add_column("File Name", style="green")
    table.add_column("Rows", style="blue", justify="right")
    table.add_column("Status", style="bold")

    # Populate the table
    for item in summary:
        table.add_row(item["File Name"], str(item["Rows"]), item["Status"])

    console.print("\n", table)

# Process and upload inventory data
process_and_upload_files(folder_path, table_name)
