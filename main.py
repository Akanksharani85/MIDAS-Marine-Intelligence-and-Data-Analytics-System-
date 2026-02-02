# main.py (Final Version with Flask)

import os
import pandas as pd
import psycopg2
from google.cloud import storage
from flask import Flask, request

app = Flask(__name__)

# =================================================================
# ==> DATABASE CONNECTION DETAILS
# 🚨 Make sure these details are correct from your Transaction Pooler
DB_HOST = "aws-1-ap-south-1.pooler.supabase.com"
DB_NAME = "postgres"
DB_USER = "postgres.ehztxnqckxrhcdnnmiuv" 
DB_PASSWORD = "Akarads@85"
DB_PORT = "6543"
# =================================================================

def process_and_insert(df):
    """Connects to the database and inserts data from a DataFrame."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        print("Database connection successful!")
        cur = conn.cursor()

        for index, row in df.iterrows():
            sql_query = """
                INSERT INTO "OceanographicData" 
                ("Date", "Location", "Temperature_Celsius", "Salinity_PSU", "Fish_Species_Count") 
                VALUES (%s, %s, %s, %s, %s);
            """
            cur.execute(sql_query, (
                row['Date'], 
                row['Location'], 
                row['Temperature_Celsius'], 
                row['Salinity_PSU'], 
                row['Fish_Species_Count']
            ))

        conn.commit()
        print(f"{len(df)} rows successfully inserted into OceanographicData table.")

    except Exception as e:
        print(f"An error occurred during database operation: {e}")
        raise e  # Re-raise the exception to signal an error
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

@app.route("/", methods=["POST"])
def main_trigger():
    """
    This is the main function that gets triggered by a new file upload.
    The event data is delivered as an HTTP POST request.
    """
    event_data = request.get_json()
    print(f"Full event data: {event_data}")

    try:
        bucket_name = event_data['bucket']
        file_name = event_data['name']
        
        print(f"Processing file: {file_name} from bucket: {bucket_name}.")

        if not file_name.startswith('datasets/'):
            print(f"File {file_name} is not in the 'datasets/' folder, skipping.")
            return ("Skipped file", 200)

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        temp_file_path = f"/tmp/{os.path.basename(file_name)}"
        blob.download_to_filename(temp_file_path)
        
        print(f"File downloaded to {temp_file_path}.")

        df = pd.read_csv(temp_file_path)
        print("CSV file read into DataFrame successfully.")
        
        process_and_insert(df)

        return ("Successfully processed file", 200)

    except Exception as e:
        print(f"An error occurred: {e}")
        return (f"Error processing file: {e}", 500)

if __name__ == "__main__":
    # This part is for local testing and is what the health checker looks for
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
