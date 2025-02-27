import pandas as pd
import gspread
import snowflake.connector
import json
import re
from datetime import datetime
from google.oauth2.service_account import Credentials

# Configuration
GSHEET_ID = "1KOVKj3n-wF2yJNeaILAbRSREvY2YhtFtg56t7MCeUvo"
SNOWFLAKE_CREDENTIALS_FILE = r"C:\Users\rcruz\Documents\GLG\Python\Credentials\snflk_ROBINSONC_creds.json"
GOOGLE_CREDENTIALS_FILE = r"C:\Users\rcruz\Documents\GLG\Python\Credentials\rs-python-scripts-001-b279ccb0d2c0.json"
SNOWFLAKE_TABLE = "GLG_ICON_NEGOTIATIONS.SCRUB_REPORTING.TBL_CREDITORS_SCRUB_MAPPING"

# Authenticate and open Google Sheet
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scope)
client = gspread.authorize(creds)
spreadsheet = client.open_by_key(GSHEET_ID)

# Identify the latest tab based on YYYYMMDD format
date_tabs = []
date_pattern = re.compile(r"^\d{8}$")  # Matches YYYYMMDD format

for sheet in spreadsheet.worksheets():
    if date_pattern.match(sheet.title):  
        date_tabs.append(sheet.title)

if not date_tabs:
    raise ValueError("No valid YYYYMMDD formatted sheets found.")

latest_tab = max(date_tabs)  # Get the latest date-based tab
print(f"Latest sheet identified: {latest_tab}")

# Extract data from columns A:C
worksheet = spreadsheet.worksheet(latest_tab)
data = worksheet.get("A:C")  # Fetch all rows in A:C

# Convert to DataFrame and filter empty rows based on Column A (ID)
df = pd.DataFrame(data[1:], columns=["ID", "Creditor", "Scrub_CR"])  # Skip first row as headers
df = df[df["ID"].notna()]  # Filter rows where Column A (ID) is not empty

# Convert ID column to integer
df["ID"] = df["ID"].astype(int)

# Evaluate Scrub_CR: Convert empty/null values to False, and ensure it's Boolean
df["Scrub_CR"] = df["Scrub_CR"].apply(lambda x: True if str(x).strip().lower() == "true" else False)

# Add Last_Update column with today's date
df["Last_Update"] = datetime.today().strftime('%Y-%m-%d')

# Load Snowflake credentials
with open(SNOWFLAKE_CREDENTIALS_FILE, "r") as file:
    sf_creds = json.load(file)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=sf_creds["user"],
    password=sf_creds["password"],
    account=sf_creds["account"],
    warehouse=sf_creds["warehouse"],
    database=sf_creds["database"],
    schema=sf_creds["schema"]
)

# Truncate the Snowflake table
truncate_query = f"TRUNCATE TABLE {SNOWFLAKE_TABLE}"
cursor = conn.cursor()
cursor.execute(truncate_query)
print("Snowflake table truncated.")

# Insert data into Snowflake, ensuring correct types
insert_query = f"INSERT INTO {SNOWFLAKE_TABLE} (ID, Creditor, Scrub_CR, Last_Update) VALUES (%s, %s, %s, %s)"
cursor.executemany(insert_query, df.values.tolist())
print("Data successfully inserted into Snowflake.")

# Close connections
cursor.close()
conn.close()
print("Process completed.")