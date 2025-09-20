from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

def fetch_currency_data():
    """Получает данные о курсах валют с API Центробанка РФ"""
    try:
        url = "https://www.cbr-xml-daily.ru/daily_json.js"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        currencies = []
        
        # Добавляем рубль как базовую валюту
        currencies.append({
            'ID': 'R00000',
            'NumCode': '643',
            'CharCode': 'RUB',
            'Nominal': 1,
            'Name': 'Российский рубль',
            'Value': 1.0,
            'Previous': 1.0,
            'Date': datetime.now().strftime("%Y-%m-%d")
        })
        
        # Добавляем все остальные валюты
        for currency_data in data['Valute'].values():
            currency_data['Date'] = datetime.now().strftime("%Y-%m-%d")
            currencies.append(currency_data)
        
        logging.info(f"Получено {len(currencies)} валют")
        return currencies
        
    except Exception as e:
        logging.error(f"Ошибка при получении данных: {str(e)}")
        raise

def validate_and_insert_data(**context):
    """Валидирует данные и вставляет их в базу данных"""
    try:
        # Получаем данные из предыдущей задачи
        currency_data = context['task_instance'].xcom_pull(task_ids='fetch_data')
        
        if not currency_data:
            raise ValueError("Данные о валютах не получены")
        
        # Подключаемся к базе данных
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Подготавливаем данные для вставки
        insert_data = []
        for currency in currency_data:
            insert_data.append((
                currency['CharCode'],
                currency['Name'],
                currency['Value'],
                currency['Nominal'],
                currency['Date']
            ))
        
        # Вставляем данные
        insert_sql = """
        INSERT INTO currency_rates (char_code, name, value, nominal, date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (char_code, date) DO UPDATE SET
            name = EXCLUDED.name,
            value = EXCLUDED.value,
            nominal = EXCLUDED.nominal
        """
        
        hook.insert_rows(table='currency_rates', rows=insert_data, target_fields=['char_code', 'name', 'value', 'nominal', 'date'])
        
        logging.info(f"Успешно вставлено {len(insert_data)} записей")
        return f"Обработано {len(insert_data)} валют"
        
    except Exception as e:
        logging.error(f"Ошибка при вставке данных: {str(e)}")
        raise

with DAG(
    'currency_etl', 
    default_args=default_args,
    description='ETL процесс для загрузки курсов валют ЦБ РФ',
    schedule='@daily',  
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['currency', 'cbr', 'etl']
) as dag:
    
    # Создание таблицы для курсов валют
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS currency_rates (
            id SERIAL PRIMARY KEY,
            char_code VARCHAR(10) NOT NULL,
            name VARCHAR(200) NOT NULL,
            value DECIMAL(20,4) NOT NULL,
            nominal INTEGER NOT NULL,
            date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(char_code, date)
        );
        
        -- Создаем индексы для оптимизации запросов
        CREATE INDEX IF NOT EXISTS idx_currency_rates_date ON currency_rates(date);
        CREATE INDEX IF NOT EXISTS idx_currency_rates_char_code ON currency_rates(char_code);
        CREATE INDEX IF NOT EXISTS idx_currency_rates_char_code_date ON currency_rates(char_code, date);
        """
    )
    
    # Получение данных с API
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_currency_data,
        doc_md="""
        ### Задача получения данных
        
        Эта задача получает актуальные курсы валют с официального API Центробанка РФ.
        
        **Источник данных:** https://www.cbr-xml-daily.ru/daily_json.js
        """
    )
    
    # Вставка данных в базу
    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=validate_and_insert_data,
        doc_md="""
        ### Задача вставки данных
        
        Эта задача валидирует полученные данные и вставляет их в базу данных.
        Использует ON CONFLICT для обновления существующих записей.
        """
    )
    
    # Создаем зависимости между задачами
    create_table >> fetch_data >> insert_data
