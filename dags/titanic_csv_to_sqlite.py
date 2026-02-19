from datetime import datetime, timedelta
import sqlite3
import urllib.request
import csv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

SQLITE_DB_PATH = '/opt/airflow/sqlite/titanic.db'
CSV_URL = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'


def download_csv(**context):
    ti = context['ti']
    csv_path = '/opt/airflow/titanic.csv'

    urllib.request.urlretrieve(CSV_URL, csv_path)
    ti.xcom_push(key='csv_path', value=csv_path)

    return csv_path


def create_table():
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS titanic')

    cursor.execute('''
        CREATE TABLE titanic (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Survived INTEGER,
            Pclass INTEGER,
            Name TEXT,
            Sex TEXT,
            Age REAL,
            Siblings_Spouses_Aboard INTEGER,
            Parents_Children_Aboard INTEGER,
            Fare REAL
        )
    ''')

    conn.commit()
    conn.close()


def load_csv_to_sqlite(**context):
    ti = context['ti']
    csv_path = ti.xcom_pull(key='csv_path', task_ids='download_csv')

    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        rows = []
        for row in reader:
            rows.append((
                int(row['Survived']),
                int(row['Pclass']),
                row['Name'],
                row['Sex'],
                float(row['Age']) if row['Age'] else None,
                int(row['Siblings/Spouses Aboard']),
                int(row['Parents/Children Aboard']),
                float(row['Fare']) if row['Fare'] else None,
            ))

        cursor.executemany('''
            INSERT INTO titanic (
                Survived, Pclass, Name, Sex, Age,
                Siblings_Spouses_Aboard, Parents_Children_Aboard, Fare
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', rows)

    conn.commit()

    cursor.execute('SELECT COUNT(*) FROM titanic')
    count = cursor.fetchone()[0]

    conn.close()

    if os.path.exists(csv_path):
        os.remove(csv_path)

    return count


def verify_data():
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM titanic')
    total = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM titanic WHERE Survived = 1')
    survived = cursor.fetchone()[0]

    print(f"Total: {total}, Survived: {survived}")

    conn.close()


with DAG(
    'titanic_csv_to_sqlite',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=None,
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv,
        provide_context=True,
    )

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    load_data_task = PythonOperator(
        task_id='load_csv_to_sqlite',
        python_callable=load_csv_to_sqlite,
        provide_context=True,
    )

    verify_task = PythonOperator(
        task_id='verify_data',
        python_callable=verify_data,
    )

    download_task >> create_table_task >> load_data_task >> verify_task
