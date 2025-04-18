import psycopg2
import pandas as pd
import numpy as np
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL connection details
host = 'localhost'
database = 'Shakedeal'
user = 'postgres'
password = '2705'
table_name = 'consignment_data'

# File path
file_path = r'C:\Users\Ranjith\OneDrive - MSFT\Shakedeal Documents\Report Files\Consignment_Data\Consignment_Data.csv'

# Read the CSV file
df = pd.read_csv(file_path)

# Function to infer PostgreSQL data types
def infer_pg_dtype(dtype):
    if np.issubdtype(dtype, np.integer):
        return "INTEGER"
    elif np.issubdtype(dtype, np.floating):
        return "FLOAT"
    elif np.issubdtype(dtype, np.bool_):
        return "BOOLEAN"
    elif np.issubdtype(dtype, np.datetime64):
        return "TIMESTAMP"
    else:
        return "TEXT"

# Establish connection to PostgreSQL
conn = None
cursor = None

try:
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    cursor = conn.cursor()

    # Check if the table exists
    check_table_query = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = '{table_name.lower()}'
    );
    """
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        logger.info(f"Table '{table_name}' does not exist. Creating it now...")
        
        # Define columns with appropriate data types
        columns_with_types = ", ".join([f'"{col}" {infer_pg_dtype(df[col].dtype)}' for col in df.columns])
        
        # Create table query
        create_table_query = f"CREATE TABLE {table_name} ({columns_with_types});"
        cursor.execute(create_table_query)
        conn.commit()
        logger.info(f"Table '{table_name}' created successfully.")

    else:
        # Check for missing columns and add them dynamically
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name.lower()}';")
        existing_columns = {row[0] for row in cursor.fetchall()}

        # Find columns in CSV but missing in DB
        missing_columns = set(df.columns) - existing_columns

        for col in missing_columns:
            logger.info(f"Adding missing column: {col}")
            alter_query = f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT;'  # Default type as TEXT
            cursor.execute(alter_query)
            conn.commit()

    # Truncate the existing table to clear old data
    truncate_query = f"TRUNCATE TABLE {table_name};"
    cursor.execute(truncate_query)
    conn.commit()

    # Prepare the insert query
    insert_query = f"""
    INSERT INTO {table_name} ({', '.join([f'"{col}"' for col in df.columns])})
    VALUES ({', '.join(['%s' for _ in df.columns])});
    """

    # Convert DataFrame to a list of tuples
    data_tuples = [tuple(row) for row in df.to_numpy()]

    # Insert data into the table
    cursor.executemany(insert_query, data_tuples)
    conn.commit()
    logger.info("Data replaced successfully in the existing table.")

except Exception as e:
    logger.error("Error:", exc_info=e)
    if conn:
        conn.rollback()

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

