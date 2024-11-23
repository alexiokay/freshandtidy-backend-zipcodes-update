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


if platform.system() == "Linux" and "WSL2" in platform.uname().release:
    # Running on WSL2
    sqlite_file = "/mnt/c/Users/alexispace/Desktop/bag.sqlite"
else:
    # Default path
    sqlite_file = "bag.sqlite"
    
csv_file = "bag.csv"

if not DATABASE_URL or not BAG_URL:
    raise Exception("DATABASE_URL or BAG_URL is missing in the .env file")

# Create TEMP_DIR if it doesn't exist
os.makedirs(TEMP_DIR, exist_ok=True)

# Ensure bag.zip exists in the script's directory
def ensure_bag_zip():
    if not os.path.exists(ZIP_FILE_NAME) or not zip_file_is_valid():
        print(f"{ZIP_FILE_NAME} not found or is invalid. Downloading...")
        download_file_with_progress(BAG_URL)

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
        df = pd.read_csv(csv_file)
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO gov_data (column1, column2, ...) 
                VALUES (%s, %s, ...)
                ON CONFLICT (id) DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, ...;
                """,
                (row["column1"], row["column2"], ...)
            )
        conn.commit()
        print("gov_data table updated successfully.")
    finally:
        conn.close()

# Main function to check and update BAG data
def check_and_update_bag_data():
    ensure_bag_zip()
    clone_bag_parse_repo()
    update_config()
    csv_file = convert_bag_to_csv(ZIP_FILE_NAME)
    update_gov_data_table(csv_file)

# Run the main function
check_and_update_bag_data()
