from datetime import datetime, timedelta
import sqlite3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

SQLITE_DB_PATH = '/opt/airflow/sqlite/titanic.db'
CSV_URL = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'


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


def load_csv_to_sqlite():
    df = pd.read_csv(CSV_URL)
    df.columns = [
        'Survived', 'Pclass', 'Name', 'Sex', 'Age',
        'Siblings_Spouses_Aboard', 'Parents_Children_Aboard', 'Fare'
    ]

    conn = sqlite3.connect(SQLITE_DB_PATH)
    df.to_sql('titanic', conn, if_exists='append', index=False)
    conn.close()

    return len(df)


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

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    load_data_task = PythonOperator(
        task_id='load_csv_to_sqlite',
        python_callable=load_csv_to_sqlite,
    )

    verify_task = PythonOperator(
        task_id='verify_data',
        python_callable=verify_data,
    )

    create_table_task >> load_data_task >> verify_task
