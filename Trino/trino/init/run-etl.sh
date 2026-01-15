#!/bin/bash
set -e

TRINO_HOST="trino"
TRINO_PORT=8080
ETL_FILE="/etc/trino/init/etl.sql"

# Ждём, пока Trino будет готов
echo "Waiting for Trino to be ready..."
until curl -s http://$TRINO_HOST:$TRINO_PORT/v1/info > /dev/null; do
  echo "Trino not ready yet. Waiting 5 seconds..."
  sleep 5
done

echo "Trino is ready! Running ETL..."

# Отправляем SQL на выполнение
SQL=$(cat $ETL_FILE)
curl -s -X POST \
  -H "X-Trino-User: user" \
  -H "X-Trino-Catalog: clickhouse_target" \
  -H "X-Trino-Schema: star" \
  -d "$SQL" \
  http://$TRINO_HOST:$TRINO_PORT/v1/statement

echo "ETL finished!"
