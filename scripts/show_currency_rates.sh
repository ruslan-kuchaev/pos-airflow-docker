#!/bin/bash

echo "💰 Просмотр курсов валют ЦБ РФ"
echo "================================"

# Проверяем, что контейнеры запущены
if ! docker-compose ps | grep -q "Up"; then
    echo "❌ Контейнеры не запущены. Запускаем..."
    docker-compose up -d
    echo "⏳ Ожидаем запуска сервисов..."
    sleep 10
fi

echo ""
echo "📊 Запускаем скрипт просмотра курсов валют..."
echo ""

# Запускаем скрипт просмотра курсов
docker-compose exec airflow-apiserver python /opt/airflow/dags/view_currency_rates.py "$@"

echo ""
echo "🌐 Веб-интерфейсы:"
echo "   • Airflow Web UI: http://localhost:8080 (airflow/airflow)"
echo "   • Metabase: http://localhost:3000"
echo ""
echo "💡 Полезные команды:"
echo "   • Просмотр истории USD: $0 --history"
echo "   • Перезапуск DAG: docker-compose exec airflow-apiserver airflow dags trigger currency_etl"
echo "   • Остановка проекта: docker-compose down"
