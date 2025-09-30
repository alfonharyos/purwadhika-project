#!/bin/bash
set -e

until pg_isready -h postgres -p 5432 -U "$AIRFLOW_DB_USER"; do
  echo "Waiting for Postgres..."
  sleep 3
done

: "${_AIRFLOW_WWW_USER_USERNAME:?Missing env variable}"
: "${_AIRFLOW_WWW_USER_PASSWORD:?Missing env variable}"
: "${_AIRFLOW_WWW_USER_FIRSTNAME:?Missing env variable}"
: "${_AIRFLOW_WWW_USER_LASTNAME:?Missing env variable}"
: "${_AIRFLOW_WWW_USER_EMAIL:?Missing env variable}"

echo "Initializing Airflow DB..."
airflow db init

if ! airflow users list | grep -q "$_AIRFLOW_WWW_USER_USERNAME"; then
    echo "Creating admin user..."
    airflow users create \
        --username "$_AIRFLOW_WWW_USER_USERNAME" \
        --password "$_AIRFLOW_WWW_USER_PASSWORD" \
        --firstname "$_AIRFLOW_WWW_USER_FIRSTNAME" \
        --lastname "$_AIRFLOW_WWW_USER_LASTNAME" \
        --role Admin \
        --email "$_AIRFLOW_WWW_USER_EMAIL"
else
    echo "Airflow admin user already exists, skipping create."
fi


echo "Running extra init tasks..."

python - <<'PYCODE'
import nltk
nltk.download("punkt")
nltk.download('punkt_tab')
nltk.download("stopwords")
print("NLTK corpora downloaded.")
PYCODE
