import csv
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# File paths within the Airflow Docker environment
CSV_FILE_PATH = "/opt/airflow/data/customers.csv"
LOADED_TXT_PATH = "/opt/airflow/output/customers_loaded.txt"
EMAILS_TXT_PATH = "/opt/airflow/output/emails_sent.txt"

def extract_customers(ti):
    """Reads the CSV file and pushes the list of customers to XCom."""
    if not os.path.exists(CSV_FILE_PATH):
        raise FileNotFoundError(f"Missing source file at {CSV_FILE_PATH}")
        
    customers = []
    with open(CSV_FILE_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Basic validation: ensure key data fields aren't completely blank
            if row.get("customer_id") and row.get("name") and row.get("email"):
                customers.append({
                    "customer_id": row["customer_id"].strip(),
                    "name": row["name"].strip(),
                    "email": row["email"].strip()
                })
                
    print(f"Successfully extracted {len(customers)} valid customers.")
    # Explicitly pushing to XCom with the key 'customers'
    ti.xcom_push(key="customers", value=customers)

def load_database(ti):
    """Pulls customers from XCom and writes ALL details to the mock database text file."""
    customers = ti.xcom_pull(task_ids="extract_customers", key="customers")
    if not customers:
        print("No customers found to load.")
        return

    os.makedirs(os.path.dirname(LOADED_TXT_PATH), exist_ok=True)

    with open(LOADED_TXT_PATH, mode="w", encoding="utf-8") as f:
        for customer in customers:
            # Writes full profile data: ID, Name, and Email
            f.write(f"Loaded Customer ID: {customer['customer_id']} | Name: {customer['name']} | Email: {customer['email']}\n")
            
    print("Database load complete with full records.")

def send_welcome_email(ti):
    """Pulls customers from XCom and logs personalized email transactions."""
    customers = ti.xcom_pull(task_ids="extract_customers", key="customers")
    if not customers:
        print("No emails to send.")
        return

    os.makedirs(os.path.dirname(EMAILS_TXT_PATH), exist_ok=True)

    with open(EMAILS_TXT_PATH, mode="w", encoding="utf-8") as f:
        for customer in customers:
            # Creates a clean, structured mock email output
            f.write(f"To: {customer['email']}\n")
            f.write(f"Subject: Welcome aboard, {customer['name']}!\n")
            f.write(f"Body: Hi {customer['name']}, thanks for signing up. Your account ID is {customer['customer_id']}.\n")
            f.write("-" * 40 + "\n")
            
    print("All detailed welcome emails have been logged!")

# Define the DAG configuration
with DAG(
    dag_id="customer_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "etl"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers
    )

    load_db_task = PythonOperator(
        task_id="load_database",
        python_callable=load_database
    )

    send_email_task = PythonOperator(
        task_id="send_welcome_email",
        python_callable=send_welcome_email
    )

    # Execution Flow: Extract -> Load Database -> Send Welcome Emails
    extract_task >> load_db_task >> send_email_task