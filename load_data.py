import os
import io
import requests
import zipfile
import psycopg2
import csv
import json
import gspread

# --- PART 1: DOWNLOAD & PROCESS ---
print("Fetching zipped data from OpenPowerlifting...")
url = "https://openpowerlifting.gitlab.io/opl-csv/files/openpowerlifting-latest.zip"
response = requests.get(url)
zip_data = zipfile.ZipFile(io.BytesIO(response.content))

# Find the CSV file
csv_filename = None
for name in zip_data.namelist():
    if name.endswith('.csv'):
        csv_filename = name
        break

if csv_filename is None:
    raise Exception("No CSV file found in the zip archive.")
print(f"Download complete. Found file: {csv_filename}")

# Buffer to store clean data for upload
filtered_buffer = io.StringIO()
clean_header = []

# Open CSV and Filter
print("Processing data (filtering for TotalKg >= 1000)...")
with zip_data.open(csv_filename, mode='r') as f:
    text_file = io.TextIOWrapper(f, encoding='utf-8')
    reader = csv.reader(text_file)
    
    # Read Header
    header = next(reader)
    clean_header = [col.replace('"', '') for col in header]
    
    # Prepare Buffer
    writer = csv.writer(filtered_buffer)
    writer.writerow(clean_header) # Write header to buffer

    # Find "TotalKg" column index
    try:
        total_index = clean_header.index("TotalKg")
    except ValueError:
        raise Exception("Could not find 'TotalKg' column!")

    # Filter Loop
    row_count = 0
    kept_count = 0
    
    for row in reader:
        row_count += 1
        # Safety check for short rows
        if len(row) <= total_index:
            continue
            
        val_text = row[total_index]
        try:
            val_float = float(val_text)
            if val_float >= 1000.0:
                writer.writerow(row)
                kept_count += 1
        except ValueError:
            continue

    print(f"Filtering done. Kept {kept_count} rows out of {row_count}.")

# --- PART 2: UPLOAD TO NEON (Postgres) ---
print("Connecting to Neon...")
conn = psycopg2.connect(os.environ["NEON_DB_URL"])
cur = conn.cursor()

# Create Table logic
sql_columns = [f'"{col}" TEXT' for col in clean_header]
columns_string = ", ".join(sql_columns)

# We use TRUNCATE to empty the table without breaking the View connection
cur.execute(f"CREATE TABLE IF NOT EXISTS raw_openpowerlifting ({columns_string});")
cur.execute("TRUNCATE TABLE raw_openpowerlifting;")
conn.commit()
print("Table truncated and ready.")

# Upload to Neon
filtered_buffer.seek(0) # Rewind buffer to start
cur.copy_expert(
    sql="COPY raw_openpowerlifting FROM STDIN WITH (FORMAT CSV, HEADER)",
    file=filtered_buffer
)
conn.commit()
print("Success! Data uploaded to Neon.")
cur.close()
conn.close()

# --- PART 3: UPLOAD TO GOOGLE SHEETS ---
print("Connecting to Google Sheets...")

if "GCP_SA_KEY" not in os.environ:
    print("Skipping Google Sheets upload (No GCP_SA_KEY found).")
else:
    try:
        # Authenticate using the JSON key from GitHub Secrets
        key_dict = json.loads(os.environ["GCP_SA_KEY"])
        gc = gspread.service_account_from_dict(key_dict)
        
        # Open the Sheet named 'powerlifting_data'
        sh = gc.open("powerlifting_data")
        worksheet = sh.sheet1
        
# ... inside the Google Sheets section ...
        
        print("Preparing data for Google Sheets...")
        filtered_buffer.seek(0)
        csv_reader = csv.reader(filtered_buffer)
        
        # Helper: Try to turn text into a number
        def auto_convert(cell):
            try:
                return float(cell)
            except ValueError:
                return cell # If it fails (like a name), keep it as text

        # Convert the data
        raw_data = list(csv_reader)
        header = raw_data[0]
        rows = raw_data[1:]
        
        # Apply conversion to every cell in every row
        typed_rows = [[auto_convert(cell) for cell in row] for row in rows]
        
        # Combine back
        final_data = [header] + typed_rows
        
        # Clear and Update
        print(f"Uploading {len(final_data)} rows to Google Sheets...")
        worksheet.clear()
        worksheet.update(final_data)
        
    except Exception as e:
        print(f"Google Sheets Error: {e}")
