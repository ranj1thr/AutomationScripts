# AutomationScripts

A collection of scripts for uploading data from local folders to a PostgreSQL database. Each script is tailored for a specific data source or table and helps automate the process of preparing, cleaning, and inserting data into your database.

## Scripts Included

- **Amazon_Fulfilled_Shipments.py**: Selects one or more Amazon FBA shipment CSV files from a local folder, combines them, removes duplicates based on a unique key, and uploads new records to the `Amazon_Fulfilled_Shipments` table in PostgreSQL.
- **Consignment_Data.py**: Reads a consignment data CSV file, ensures the `consignment_data` table exists (creating or altering it as needed), truncates old data, and uploads the new data to PostgreSQL.
- **FBA Data.py**: Scans a folder for Amazon FBA inventory CSV files, dynamically creates the `FBA_Inventory` table if needed, truncates old data, and uploads all new inventory data to PostgreSQL.
- **FBF_Inventory Upload.py**: Finds all Flipkart inventory CSVs, ensures the `FBF_Inventory` table exists, truncates it, and uploads the new data to PostgreSQL.
- **Inventory_to_Postgres.py**: Recursively scans a folder for inventory files (CSV or Excel), creates the `Inventory` table if needed, truncates it, and uploads all inventory data to PostgreSQL, showing a summary table of the process.

## Usage

1. Clone this repository:
   ```sh
   git clone https://github.com/ranj1thr/AutomationScripts.git
   ```
2. Navigate to the `public_scripts` folder:
   ```sh
   cd AutomationScripts/public_scripts
   ```
3. Run any script using Python:
   ```sh
   python <script_name.py>
   ```

> **Note:** Update the database connection details in each script as needed for your environment.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.