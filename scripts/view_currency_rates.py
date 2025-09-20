#!/usr/bin/env python3
"""
Скрипт для просмотра курсов валют из базы данных
"""

import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import sys

def connect_to_db():
    """Подключение к базе данных"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

def get_latest_rates():
    """Получение последних курсов валют"""
    conn = connect_to_db()
    if not conn:
        return None
    
    try:
        query = """
        SELECT char_code, name, value, date
        FROM currency_rates 
        WHERE date = (SELECT MAX(date) FROM currency_rates)
        ORDER BY value DESC
        """
        
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None
    finally:
        conn.close()

def get_currency_history(char_code, days=30):
    """Получение истории курса валюты"""
    conn = connect_to_db()
    if not conn:
        return None
    
    try:
        query = """
        SELECT date, value
        FROM currency_rates 
        WHERE char_code = %s 
            AND date >= %s
        ORDER BY date DESC
        LIMIT %s
        """
        
        start_date = datetime.now() - timedelta(days=days)
        df = pd.read_sql_query(query, conn, params=[char_code, start_date, days])
        return df
    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None
    finally:
        conn.close()

def get_top_volatile_currencies(days=7):
    """Получение самых волатильных валют"""
    conn = connect_to_db()
    if not conn:
        return None
    
    try:
        query = """
        SELECT 
            char_code,
            COUNT(*) as records,
            MIN(value) as min_value,
            MAX(value) as max_value,
            ROUND((MAX(value) - MIN(value)) / MIN(value) * 100, 2) as volatility_percent
        FROM currency_rates 
        WHERE date >= %s
        GROUP BY char_code
        HAVING COUNT(*) >= %s
        ORDER BY volatility_percent DESC
        LIMIT 10
        """
        
        start_date = datetime.now() - timedelta(days=days)
        df = pd.read_sql_query(query, conn, params=[start_date, days])
        return df
    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
        return None
    finally:
        conn.close()

def main():
    """Основная функция"""
    print("💰 Курсы валют ЦБ РФ")
    print("=" * 50)
    
    # Показать последние курсы
    print("\n📊 Последние курсы валют:")
    rates = get_latest_rates()
    if rates is not None and not rates.empty:
        print(rates.to_string(index=False))
    else:
        print("❌ Данные не найдены")
        return
    
    # Показать топ-10 валют
    print(f"\n🏆 Топ-10 валют по курсу:")
    top_rates = rates.head(10)
    print(top_rates.to_string(index=False))
    
    # Показать основные валюты
    print(f"\n💵 Основные валюты:")
    main_currencies = rates[rates['char_code'].isin(['USD', 'EUR', 'GBP', 'CNY', 'JPY'])]
    if not main_currencies.empty:
        print(main_currencies.to_string(index=False))
    
    # Показать волатильность
    print(f"\n📈 Самые волатильные валюты за последние 7 дней:")
    volatile = get_top_volatile_currencies(7)
    if volatile is not None and not volatile.empty:
        print(volatile.to_string(index=False))
    
    # Показать историю USD если запрошено
    if len(sys.argv) > 1 and sys.argv[1] == "--history":
        print(f"\n📉 История курса USD за последние 30 дней:")
        usd_history = get_currency_history('USD', 30)
        if usd_history is not None and not usd_history.empty:
            print(usd_history.to_string(index=False))
    
    print(f"\n💡 Для просмотра истории USD используйте: python {sys.argv[0]} --history")
    print(f"🌐 Веб-интерфейс Airflow: http://localhost:8080")
    print(f"📊 Дашборды Metabase: http://localhost:3000")

if __name__ == "__main__":
    main()
