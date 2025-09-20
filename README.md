# 💰 Currency ETL Pipeline

Проект автоматически загружает ежедневные курсы валют с API Центробанка РФ, сохраняет их в PostgreSQL и предоставляет инструменты для визуализации через Metabase.

## 🚀 Возможности

- **Автоматическая загрузка**: Ежедневная загрузка курсов валют с официального API ЦБ РФ
- **Надежное хранение**: Сохранение данных в PostgreSQL с индексами для быстрых запросов
- **Визуализация**: Готовые дашборды в Metabase для анализа курсов валют
- **Мониторинг**: Полное логирование и отслеживание выполнения задач
- **Масштабируемость**: Архитектура на основе Docker и Apache Airflow

## 🏗️ Архитектура

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   API ЦБ РФ     │───▶│   Apache        │───▶│   PostgreSQL    │
│                 │    │   Airflow       │    │   Database      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   Redis         │    │   Metabase      │
                       │   (Broker)      │    │   (Dashboard)   │
                       └─────────────────┘    └─────────────────┘
```

## 📋 Требования

- Docker
- Docker Compose
- 4GB RAM (минимум)
- 10GB свободного места

## 🛠️ Установка и запуск

### 1. Клонирование репозитория
```bash
git clone <repository-url>
cd pos-airflow-docker
```

### 2. Запуск проекта
```bash
# Запуск всех сервисов
docker-compose up -d

# Проверка статуса
docker-compose ps
```

### 3. Доступ к сервисам

| Сервис | URL | Логин | Пароль |
|--------|-----|-------|--------|
| **Airflow Web UI** | http://localhost:8080 | `airflow` | `airflow` |
| **Metabase** | http://localhost:3000 | - | - |
| **PostgreSQL (Airflow)** | localhost:5432 | `airflow` | `airflow` |
| **PostgreSQL (Metabase)** | localhost:5433 | `metabase` | `metabase` |

## 📊 Данные

### Структура таблицы `currency_rates`

```sql
CREATE TABLE currency_rates (
    id SERIAL PRIMARY KEY,
    char_code VARCHAR(10) NOT NULL,        -- Код валюты (USD, EUR, etc.)
    name VARCHAR(200) NOT NULL,            -- Название валюты
    value DECIMAL(20,4) NOT NULL,          -- Курс к рублю
    nominal INTEGER NOT NULL,              -- Номинал
    date DATE NOT NULL,                    -- Дата курса
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(char_code, date)
);
```

### Примеры запросов

```sql
-- Последние курсы основных валют
SELECT char_code, name, value, date 
FROM currency_rates 
WHERE char_code IN ('USD', 'EUR', 'GBP') 
ORDER BY date DESC, char_code;

-- Динамика курса доллара за последние 30 дней
SELECT date, value 
FROM currency_rates 
WHERE char_code = 'USD' 
ORDER BY date DESC 
LIMIT 30;
```

## 🔄 ETL Процесс

### DAG: `currency_etl`

**Расписание**: Ежедневно в 00:00 UTC

**Задачи**:
1. **create_table** - Создание таблицы и индексов (если не существует)
2. **fetch_data** - Получение данных с API ЦБ РФ
3. **insert_data** - Вставка данных в базу с обработкой дубликатов

### API Центробанка РФ

- **URL**: `https://www.cbr-xml-daily.ru/daily_json.js`
- **Формат**: JSON
- **Частота обновления**: Ежедневно
- **Валюты**: 50+ валют мира

## 📈 Визуализация в Metabase

### Настройка подключения к базе данных

1. Откройте Metabase: http://localhost:3000
2. Создайте аккаунт администратора
3. Добавьте базу данных:
   - **Тип**: PostgreSQL
   - **Хост**: `postgres_airflow`
   - **Порт**: `5432`
   - **База данных**: `airflow`
   - **Пользователь**: `airflow`
   - **Пароль**: `airflow`

### Примеры дашбордов

1. **Обзор курсов валют**
   - Текущие курсы основных валют
   - Топ-10 валют по изменению курса

2. **Анализ динамики**
   - График изменения курса USD/EUR
   - Волатильность валют

3. **Статистика**
   - Количество загруженных записей
   - Статус выполнения ETL

## 🔧 Управление

### Запуск DAG вручную
```bash
docker-compose exec airflow-apiserver airflow dags trigger currency_etl
```

### Проверка статуса DAG
```bash
docker-compose exec airflow-apiserver airflow dags list-runs currency_etl
```

### Просмотр логов
```bash
docker-compose logs airflow-worker
docker-compose logs airflow-scheduler
```

### Остановка проекта
```bash
docker-compose down
```

## 🐛 Устранение неполадок

### Проблема: DAG не выполняется
```bash
# Проверьте статус контейнеров
docker-compose ps

# Перезапустите DAG processor
docker-compose restart airflow-dag-processor
```

### Проблема: Ошибки подключения к базе данных
```bash
# Проверьте подключение
docker-compose exec postgres_airflow psql -U airflow -d airflow -c "\dt"
```

### Проблема: Metabase не загружается
```bash
# Проверьте логи
docker-compose logs metabase

# Перезапустите сервис
docker-compose restart metabase
```

## 📝 Логирование

Все логи сохраняются в папке `logs/`:
- `dag_id=currency_etl/` - логи выполнения DAG
- `dag_processor/` - логи обработки DAG
- `scheduler/` - логи планировщика

## 🚀 Расширение функциональности

### Добавление новых валют
Отредактируйте функцию `fetch_currency_data()` в `dags/currency_etl.py`

### Настройка алертов
Добавьте email уведомления в `default_args` DAG

### Добавление новых источников данных
Создайте новый DAG для загрузки данных из других источников

## 📄 Лицензия

MIT License

## 👨‍💻 Автор

Проект создан для демонстрации навыков работы с:
- Apache Airflow
- PostgreSQL
- Docker
- Metabase
- Python
- ETL процессы

---

**Примечание**: Данный проект предназначен для образовательных целей и демонстрации навыков работы с современными инструментами обработки данных.
