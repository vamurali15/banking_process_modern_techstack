import os
import boto3
import snowflake.connector
import shutil # Import for local file cleanup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MinIO Config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# Snowflake Config
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customers", "accounts", "transactions"]
SNOWFLAKE_METADATA_TABLE = "LOADED_FILES_METADATA"

def download_from_minio():
    """Downloads files from MinIO to the local Airflow worker disk."""
    os.makedirs(LOCAL_DIR, exist_ok=True)
    
    # MinIO client (boto3) setup
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        # IMPORTANT: MinIO usually requires path-style addressing
        config=boto3.session.Config(signature_version='s3v4'),
    )
    
    local_files = {}
    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        
        # Only process files, not directories
        downloadable_objects = [obj for obj in objects if not obj["Key"].endswith('/')]
        
        local_files[table] = []
        for obj in downloadable_objects:
            key = obj["Key"]
            # Fix: Create a unique local file path including the table name to avoid conflicts
            local_file = os.path.join(LOCAL_DIR, table, os.path.basename(key)) 
            os.makedirs(os.path.join(LOCAL_DIR, table), exist_ok=True)
            
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)
            
    # XCom Push: local_files is returned implicitly by Airflow
    return local_files

def load_to_snowflake(**kwargs):
    """Loads downloaded files into Snowflake, tracks metadata, and cleans up."""
    ti = kwargs["ti"]
    local_files = ti.xcom_pull(task_ids="download_minio")
    
    if not local_files:
        print("No files found in MinIO or XCom.")
        return

    # Store files for cleanup outside the try/except block
    all_local_files = [f for files in local_files.values() for f in files]
    
    try:
        with snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA,
        ) as conn:
            # Autocommit is typically True for convenience in Python connector
            # but setting it to False (as in original code) requires explicit commit
            conn.autocommit = False 
            
            with conn.cursor() as cur:
                # 1. Ensure Metadata Table Exists
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_METADATA_TABLE} (
                        filename STRING PRIMARY KEY,
                        load_timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                """)

                # 2. Get Already Loaded Files
                cur.execute(f"SELECT filename FROM {SNOWFLAKE_METADATA_TABLE}")
                loaded_files = {row[0] for row in cur.fetchall()}

                for table, files in local_files.items():
                    if not files:
                        print(f"No files for {table}, skipping.")
                        continue

                    new_files = [f for f in files if os.path.basename(f) not in loaded_files]
                    if not new_files:
                        print(f"No new files to load for {table}.")
                        continue

                    # 3. PUT Files to Stage
                    # The PUT command must be run for each file
                    for f in new_files:
                        cur.execute(
                            # The file path is correctly local to the Airflow worker
                            f"PUT file://{f} @%{table} AUTO_COMPRESS=TRUE OVERWRITE=FALSE"
                        )
                        print(f"Uploaded {f} to Snowflake table stage @%{table}")

                    # 4. COPY Data into Target Table
                    cur.execute(f"""
                        COPY INTO {table}
                        FROM @%{table}
                        FILE_FORMAT=(TYPE=PARQUET)
                        PURGE=TRUE -- Purge staged files after successful copy
                        ON_ERROR='CONTINUE'
                    """)
                    print(f"Copied data into {table} and purged staged files.")
                    
                    # 5. Insert Metadata for Loaded Files
                    for f in new_files:
                        filename = os.path.basename(f)
                        # Use parameterized query to prevent SQL injection and error on single quote in filename
                        insert_query = f"INSERT INTO {SNOWFLAKE_METADATA_TABLE} (filename) VALUES (%s)"
                        try:
                             cur.execute(insert_query, (filename,))
                             print(f"Inserted filename into metadata: {filename}")
                        except Exception as e:
                             print(f"Failed to insert filename {filename}. It might already exist (PK conflict): {e}")


                # 6. Final Commit
                conn.commit()
                print("All tables processed successfully.")

    except Exception as e:
        print(f"Error loading data to Snowflake: {e}")
        # Rollback in case of failure
        conn.rollback() 
        raise

    finally:
        # CRITICAL FIX: Clean up the locally downloaded files to prevent disk overuse
        if os.path.exists(LOCAL_DIR):
            print(f"Cleaning up local directory: {LOCAL_DIR}")
            shutil.rmtree(LOCAL_DIR)
            
        print("Local file cleanup complete.")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_once",
    default_args=default_args,
    description="Load MinIO parquet files into Snowflake only once with metadata tracking",
    # Note: schedule_interval="*/1 * * * *" runs every minute. 
    schedule_interval="*/1 * * * *", 
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    task1 >> task2