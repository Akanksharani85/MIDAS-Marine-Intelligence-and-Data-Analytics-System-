# summarizer.py
import psycopg2
import os

# =================================================================
# ==> DATABASE CONNECTION DETAILS
# 🚨 Make sure these details are correct from your Transaction Pooler
DB_HOST = "aws-1-ap-south-1.pooler.supabase.com"
DB_NAME = "postgres"
DB_USER = "postgres.ehztxnqckxrhcdnnmiuv" 
DB_PASSWORD = "Akarads@85"
DB_PORT = "6543"
# =================================================================

def generate_summaries(request):
    """Calculates summaries and saves them to the database."""
    conn = None
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
        print("Database connection successful!")
        cur = conn.cursor()

        # --- Calculation 1: Average Temperature ---
        cur.execute('SELECT AVG("Temperature_Celsius") FROM "OceanographicData"')
        avg_temp_result = cur.fetchone()

        if avg_temp_result and avg_temp_result[0] is not None:
            avg_temp = round(avg_temp_result[0], 2) # Round to 2 decimal places
            metric_name = "Average Temperature"
            metric_value = f"{avg_temp}°C"
            trend = "↔️ Stable" # We can add trend logic later

            # Use "UPSERT" to either insert a new row or update an existing one
            upsert_query = """
                INSERT INTO "PolicyDashSummaries" (metric_name, metric_value, trend_description, region)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (metric_name) DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                trend_description = EXCLUDED.trend_description,
                last_updated = now();
            """
            cur.execute(upsert_query, (metric_name, metric_value, trend, 'All Regions'))
            print(f"Successfully updated summary for: {metric_name}")

        # You can add more calculations for other metrics here in the future

        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

# --- This allows us to run the script directly for testing ---
if __name__ == "__main__":
    print("Starting summary generation...")
    generate_summaries()
    print("Summary generation finished.")