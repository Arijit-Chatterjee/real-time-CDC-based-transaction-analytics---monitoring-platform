FROM apache/airflow:slim-3.3.2-python3.12

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-standard" \
    "docker==7.2.0" \
    --constraint "${HOME}/constraints.txt"
