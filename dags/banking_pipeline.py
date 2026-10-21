from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import random
import os

default_args = {
    'owner': 'ilya',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

DB_CONFIG = {
    'host': os.getenv("POSTGRES_HOST", "postgres"),
    'port': os.getenv("POSTGRES_PORT", 5432),
    'database': os.getenv("POSTGRES_DB", "banking_raw"),
    'user': os.getenv("POSTGRES_USER", "admin"),
    'password': os.getenv("POSTGRES_PASSWORD")
}

def load_transactions():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    categories = ['transfer', 'payment', 'withdrawal', 'deposit', 'refund']
    statuses = ['success', 'failed', 'pending']
    weights = [0.85, 0.10, 0.05]
    
    records = []
    for i in range(1000):
        records.append((
            random.randint(1, 5000),
            round(random.expovariate(1/150), 2),
            random.choice(categories),
            random.choices(statuses, weights=weights)[0],
            datetime.now()
        ))
    
    cur.executemany("""
        INSERT INTO transactions (user_id, amount, category, status, created_at)
        VALUES (%s, %s, %s, %s, %s)
    """, records)
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Загружено 1000 новых транзакций")

def check_anomalies():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT COUNT(*) FROM transactions 
        WHERE amount > 500 
        AND created_at > NOW() - INTERVAL '1 hour'
    """)
    anomalies = cur.fetchone()[0]
    print(f"⚠️ Аномальных транзакций за час: {anomalies}")
    
    cur.close()
    conn.close()

with DAG(
    dag_id='banking_pipeline',
    default_args=default_args,
    description='Загрузка и проверка банковских транзакций',
    schedule_interval='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False
) as dag:

    task_load = PythonOperator(
        task_id='load_transactions',
        python_callable=load_transactions
    )

    task_check = PythonOperator(
        task_id='check_anomalies',
        python_callable=check_anomalies
    )

    task_load >> task_check
