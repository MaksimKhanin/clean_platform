#!/usr/bin/env bash

# Initiliase the metastore
airflow initdb

# Run the scheduler in background
airflow scheduler &> /dev/null &

python ./cfg_parser.py ${POSTGRES_USER} ${POSTGRES_PASSWORD} ${POSTGRES_HOST} ${POSTGRES_PORT} ${AIRFLOW_SECRET_KEY}
airflow users create --email ${AIRFLOW_EMAIL} --firstname admin --lastname admin --password ${AIRFLOW_ADMIN_PASSWORD} --role Admin --username ${AIRFLOW_ADMIN_USER}

# Run the web server in foreground (for docker logs)
exec airflow webserver



