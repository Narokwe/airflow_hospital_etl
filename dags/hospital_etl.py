from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import logging

# Database connection details
DB_HOST = "localhost"
DB_NAME = "healthcare_db"
DB_USER = "postgres"
DB_PASSWORD = "Augustine123"
DB_PORT = "5432"

# File paths (update as per WSL structure)
BASE_PATH = "/mnt/wsl/Ubuntu/home/postgres/airflow_hospital_etl/data/"
RAW_FILE = BASE_PATH + "hospital_admissions.csv"
PROCESSED_FILE = BASE_PATH + "processed_admissions.csv"
FINAL_FILE = BASE_PATH + "final_admissions.csv"

# Configure logging
logger = logging.getLogger(__name__)

# Extract Function with error handling
def extract_data(**kwargs):
    try:
        logger.info("Starting data extraction...")
        df = pd.read_csv(RAW_FILE)
        df.to_csv(PROCESSED_FILE, index=False)
        logger.info(f"Successfully extracted data to {PROCESSED_FILE}")
        return "Extraction completed successfully"
    except Exception as e:
        error_msg = f"Extraction failed: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

# Transform Function with error handling
def transform_data(**kwargs):
    try:
        logger.info("Starting data transformation...")
        df = pd.read_csv(PROCESSED_FILE)
        
        # Validate data exists
        if df.empty:
            raise ValueError("No data found in the input file")
            
        # Data processing
        df['admission_date'] = pd.to_datetime(df['admission_date'], errors='coerce')
        df['discharge_date'] = pd.to_datetime(df['discharge_date'], errors='coerce')
        
        # Check for invalid dates
        if df['admission_date'].isnull().any() or df['discharge_date'].isnull().any():
            invalid_count = df['admission_date'].isnull().sum() + df['discharge_date'].isnull().sum()
            logger.warning(f"Found {invalid_count} records with invalid dates")
            
        df.to_csv(FINAL_FILE, index=False)
        logger.info(f"Successfully transformed data to {FINAL_FILE}")
        return "Transformation completed successfully"
    except Exception as e:
        error_msg = f"Transformation failed: {str(e)}"
        logger.error(error_msg)
        raise AirflowException(error_msg)

# Load Function with error handling
def load_data(**kwargs):
    conn = None
    try:
        logger.info("Starting data loading...")
        df = pd.read_csv(FINAL_FILE)
        
        # Validate data exists
        if df.empty:
            raise ValueError("No data found to load")
            
        conn = psycopg2.connect(
            host=DB_HOST, 
            database=DB_NAME, 
            user=DB_USER, 
            password=DB_PASSWORD, 
            port=DB_PORT
        )
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'hospital_admissions'
            );
        """)
        if not cur.fetchone()[0]:
            raise AirflowException("Target table 'hospital_admissions' does not exist")
        
        # Load data
        successful_rows = 0
        for _, row in df.iterrows():
            try:
                cur.execute(
                    """INSERT INTO hospital_admissions 
                       (patient_id, admission_date, discharge_date, diagnosis, department)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (row['patient_id'], row['admission_date'], row['discharge_date'], 
                     row['diagnosis'], row['department'])
                )
                successful_rows += 1
            except Exception as e:
                logger.warning(f"Failed to insert row {_}: {str(e)}")
                conn.rollback()
                continue
                
        conn.commit()
        logger.info(f"Successfully loaded {successful_rows}/{len(df)} records")
        return f"Loading completed with {successful_rows} records"
        
    except Exception as e:
        error_msg = f"Loading failed: {str(e)}"
        logger.error(error_msg)
        if conn:
            conn.rollback()
        raise AirflowException(error_msg)
    finally:
        if conn:
            conn.close()

# Airflow DAG Definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 3,  # Increased from 1 to 3
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,  # Optional: Add email alerts
    'email_on_retry': False,
}

dag = DAG(
    'hospital_etl',
    default_args=default_args,
    description='ETL pipeline for hospital admissions data',
    schedule_interval='@daily',
    catchup=False,  # Prevents backfilling of old runs
    tags=['healthcare', 'etl'],
)

t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
    provide_context=True  # Allows access to kwargs in the function
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
    provide_context=True
)

t3 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
    provide_context=True
)

t1 >> t2 >> t3  # Task Execution Order