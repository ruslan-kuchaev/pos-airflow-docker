from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import requests
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def fetch_currency_data():
    url = "https://www.cbr-xml-daily.ru/daily_json.js"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data['Valute'].values())
    df['Date'] = datetime.now().strftime("%Y-%m-%d")
    return df.to_dict('records')

with DAG(
    'currency_etl', 
    default_args=default_args,
    schedule='@daily',  
    start_date=datetime(2022, 8, 12),
    catchup=False
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS currency_rates (
            id SERIAL PRIMARY KEY,
            char_code VARCHAR(10),
            name VARCHAR(100),
            value NUMERIC,
            nominal INTEGER,
            date DATE
        );
        """
    )
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_currency_data
    )
    insert_data = SQLExecuteQueryOperator(
        task_id='insert_data',
        conn_id='postgres_default',
        sql="""
        INSERT INTO currency_rates (char_code, name, value, nominal, date)
        VALUES
        {% for row in task_instance.xcom_pull(task_ids='fetch_data') %}
            ('{{ row['CharCode'] }}', '{{ row['Name'] }}', {{ row['Value'] }}, {{ row['Nominal'] }}, '{{ row['Date'] }}'){% if not loop.last %},{% endif %}
        {% endfor %};
        """
    )
    create_table >> fetch_data >> insert_data
