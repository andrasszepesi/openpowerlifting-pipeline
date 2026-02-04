import os
import io
import requests
import zipfile
import psycopg2
import csv

# 1. DOWNLOAD
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

# 2. CONNECT
print("Connecting to Neon...")
conn = psycopg2.connect(os.environ["NEON_DB_URL"])
cur = conn.cursor()

# 3. DYNAMIC TABLE & FILTER
print("Processing data (filtering for TotalKg >= 1000)...")

# We open the CSV file inside the zip
with zip_data.open(csv_filename, mode='r') as f:
    text_file = io.TextIOWrapper(f, encoding='utf-8')
    reader = csv.reader(text_file)
    
    # Read Header
    header = next(reader)
    clean_header = [col.replace('"', '') for col in header]
    
    # Create Table Dynamically
    sql_columns = [f'"{col}" TEXT' for col in clean_header]
    columns_string = ", ".join(sql_columns)
    
    cur.execute("TRUNCATE TABLE raw_openpowerlifting;")

    conn.commit()
    print("Table truncated and ready.")

    # Prepare Buffer for Elite Lifters
    filtered_buffer = io.StringIO()
    writer = csv.writer(filtered_buffer)
    writer.writerow(clean_header) # Write header to buffer

    # Find "TotalKg" column index
    try:
        total_index = clean_header.index("TotalKg")
    except ValueError:
        print("Columns found:", clean_header)
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

# 4. UPLOAD
print("Uploading filtered data...")
filtered_buffer.seek(0) # Rewind buffer

cur.copy_expert(
    sql="COPY raw_openpowerlifting FROM STDIN WITH (FORMAT CSV, HEADER)",
    file=filtered_buffer
)

conn.commit()
print("Success! Data uploaded.")

cur.close()
conn.close()
