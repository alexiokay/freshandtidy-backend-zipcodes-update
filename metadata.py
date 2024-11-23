import psycopg2
import requests
import os
import subprocess
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm  # For progress bar during file download
import pandas as pd  # For reading and processing the CSV file
import zipfile
import platform

# Load environment variables from .env
load_dotenv()

# Fetch sensitive data from .env
DATABASE_URL = os.getenv("ZIPCODES_DATABASE_URL")
BAG_URL = os.getenv("BAG_URL")
DEBUG = os.getenv("DEBUG", "False").lower() == "true"  # Enable debug if DEBUG=True in .env
BAG_PARSE_REPO = "https://github.com/digitaldutch/BAG_parser.git"
TEMP_DIR = "bag_temp"
ZIP_FILE_NAME = "bag.zip"

# Determine SQLite file location based on platform
sqlite_file = "bag.sqlite" if not (platform.system() == "Linux" and "WSL2" in platform.uname().release) else "/mnt/c/bag.sqlite"
csv_file = "bag.csv"

if not DATABASE_URL or not BAG_URL:
    raise Exception("DATABASE_URL or BAG_URL is missing in the .env file")

# Create TEMP_DIR if it doesn't exist
os.makedirs(TEMP_DIR, exist_ok=True)

# Database connection setup
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

# Get the last modified timestamp from the metadata table
def get_last_modified_from_db(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM metadata WHERE key = 'last_modified'")
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None

# Update the last modified timestamp in the metadata table
def update_last_modified_in_db(conn, last_modified):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO metadata (key, value)
        VALUES ('last_modified', %s)
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value;
        """,
        (last_modified,)
    )
    conn.commit()
    cursor.close()

# Check if the zip file exists, is not empty, and is a valid ZIP
def zip_file_is_valid():
    if not os.path.exists(ZIP_FILE_NAME) or os.path.getsize(ZIP_FILE_NAME) <= 0:
        return False
    try:
        with zipfile.ZipFile(ZIP_FILE_NAME, 'r') as zip_file:
            return zip_file.testzip() is None
    except zipfile.BadZipFile:
        return False

# Download file with progress bar
def download_file_with_progress(url):
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download the file from {url}. Status: {response.status_code}")
    total_size = int(response.headers.get("Content-Length", 0))
    with open(ZIP_FILE_NAME, "wb") as file, tqdm(
        desc=ZIP_FILE_NAME,
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
            bar.update(len(chunk))
    print(f"File downloaded and saved as: {ZIP_FILE_NAME}")

# Clone the BAG_parse repository if not already cloned
def clone_bag_parse_repo():
    if not os.path.exists(os.path.join(TEMP_DIR, ".git")):
        print("Cloning BAG_parse repository...")
        subprocess.run(["git", "clone", BAG_PARSE_REPO, TEMP_DIR], check=True)
    else:
        print("BAG_parse repository already exists. Pulling latest changes...")
        subprocess.run(["git", "-C", TEMP_DIR, "pull"], check=True)

# Update config.py to set delete_no_longer_needed_bag_tables = True
def update_config():
    config_path = os.path.join(TEMP_DIR, "config.py")
    if not os.path.exists(config_path):
        raise Exception(f"config.py not found in {TEMP_DIR}")
    with open(config_path, "r") as file:
        lines = file.readlines()
    with open(config_path, "w") as file:
        for line in lines:
            if line.strip().startswith("delete_no_longer_needed_bag_tables"):
                file.write("delete_no_longer_needed_bag_tables = True\n")
                print("updated: delete_no_longer_needed_bag_tables")
            elif line.strip().startswith("file_db_sqlite"):
                file.write(f'file_db_sqlite = "{sqlite_file}"\n')  # Use valid Python string syntax
                print("updated: file_db_sqlite")
            else:
                file.write(line)

    print("Updated config.py")

# Convert BAG file to SQLite and then CSV using BAG_parse
def convert_bag_to_csv(bag_file):
    print("Converting BAG file to SQLite and CSV...")
    subprocess.run(["python3", os.path.join(TEMP_DIR, "import_bag.py"), bag_file, sqlite_file], check=True)
    subprocess.run(["python3", os.path.join(TEMP_DIR, "export_to_csv.py"), "-a", sqlite_file, csv_file], check=True)
    print(f"Conversion complete. CSV saved as: {csv_file}")
    return csv_file

# Update the gov_data table with the CSV file
def update_gov_data_table(csv_file):
    conn = psycopg2.connect(DATABASE_URL)
    try:
        # Load CSV file into a DataFrame
        df = pd.read_csv(csv_file)
        csv_columns = set(df.columns)

        # Fetch existing database columns
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'gov_data'
        """)
        db_columns = {row[0] for row in cursor.fetchall()}

        # Check for structural changes
        # if csv_columns != db_columns:
        #     subject = "Structural Change Detected in gov_data"
        #     body = (
        #         f"The structure of the 'gov_data' table has changed.\n\n"
        #         f"Database columns: {sorted(db_columns)}\n"
        #         f"CSV columns: {sorted(csv_columns)}\n\n"
        #         "Please review the changes."
        #     )
        #     send_email(subject, body)
        #     raise Exception("Structural changes detected. Email notification sent.")

        # Clear existing data
        cursor.execute("TRUNCATE TABLE gov_data")
        print("Existing data in 'gov_data' table cleared.")

        # Insert new data
        for _, row in df.iterrows():
            values = tuple(row[col] for col in db_columns)  # Ensure order matches DB columns
            placeholders = ", ".join(["%s"] * len(values))
            query = f"""
                INSERT INTO gov_data ({', '.join(db_columns)}) 
                VALUES ({placeholders})
            """
            cursor.execute(query, values)

        conn.commit()
        print("gov_data table updated successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        conn.close()

# Main function to check and update BAG data
def check_and_update_bag_data():
    response = requests.head(BAG_URL)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch metadata from {BAG_URL}. Status: {response.status_code}")

    last_modified_header = response.headers.get("Last-Modified")
    if not last_modified_header:
        raise Exception("Last-Modified header not found in the response")

    last_modified_online = datetime.strptime(last_modified_header, "%a, %d %b %Y %H:%M:%S GMT")
    conn = get_db_connection()
    try:
        last_modified_db = get_last_modified_from_db(conn)
        if last_modified_db:
            last_modified_db = datetime.strptime(last_modified_db, "%a, %d %b %Y %H:%M:%S GMT")
            if last_modified_online <= last_modified_db and zip_file_is_valid():
                print("No update required. The BAG data is already up-to-date.")
                return

        print("New BAG data found or DEBUG mode enabled. Proceeding with download.")
        download_file_with_progress(BAG_URL)
        clone_bag_parse_repo()
        update_config()
        csv_file = convert_bag_to_csv(ZIP_FILE_NAME)
        update_gov_data_table(csv_file)
        update_last_modified_in_db(conn, last_modified_header)
    finally:
        conn.close()

# Run the main function
check_and_update_bag_data()
