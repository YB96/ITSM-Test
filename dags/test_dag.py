from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import psycopg2
import csv
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def norm(value):
    if value is None:
        return None
    return str(value).strip()

def ingest_csv_to_postgres(**kwargs):
    csv_file_path = "/opt/airflow/data/Sample Data file for Analysis_Jan'25.xlsx"
    conn = psycopg2.connect(
        host='postgres',  # use your Postgres service name in Docker
        dbname='itsm_db',
        user='postgres',
        password='password'
    )
    cur = conn.cursor()

    # Read CSV and prepare rows
    rows = []
    with open(csv_file_path, 'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                norm(r.get('Ticket ID')),
                norm(r.get('Category')),
                norm(r.get('Sub-Category')),
                norm(r.get('Priority')),
                norm(r.get('Created Date')),
                norm(r.get('Resolved Date')),
                norm(r.get('Status')),
                norm(r.get('Assigned Group')),
                norm(r.get('Technician')),
                float(r.get('Resolution Time (Hrs)')) if r.get('Resolution Time (Hrs)') else None,
                norm(r.get('Customer Impact'))
            ))

    # Upsert into Postgres
    insert_sql = """
    INSERT INTO itsm_raw_tickets (
        ticket_id, category, sub_category, priority,
        created_date, resolved_date, status, assigned_group,
        technician, resolution_time_hrs, customer_impact
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (ticket_id) DO UPDATE SET
        category = EXCLUDED.category,
        sub_category = EXCLUDED.sub_category,
        priority = EXCLUDED.priority,
        created_date = EXCLUDED.created_date,
        resolved_date = EXCLUDED.resolved_date,
        status = EXCLUDED.status,
        assigned_group = EXCLUDED.assigned_group,
        technician = EXCLUDED.technician,
        resolution_time_hrs = EXCLUDED.resolution_time_hrs,
        customer_impact = EXCLUDED.customer_impact;
    """
    cur.executemany(insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()


with DAG(
    'itsm_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Ingest ITSM CSV, transform with DBT, and validate'
) as dag:

    # Task 1: Ingest CSV into Postgres
    ingest_task = PythonOperator(
        task_id='ingest_csv_to_postgres',
        python_callable=ingest_csv_to_postgres
    )

    # Task 2: Run DBT transformations
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            'cd /opt/airflow/dbt && '
            'dbt deps && '
            'dbt seed --profiles-dir . && '
            'dbt run --profiles-dir .'
        )
    )

    # Task 3: Validate DBT transformations
    validate_dbt = BashOperator(
        task_id='validate_dbt',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .'
    )

    # Set task dependencies
    ingest_task >> run_dbt >> validate_dbt
