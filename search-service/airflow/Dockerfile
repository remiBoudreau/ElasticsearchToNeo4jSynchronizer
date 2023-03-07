FROM apache/airflow:2.4.0

COPY --chown=airflow:root neo4j2ESDAG.py /opt/airflow/dags/neo4j2ESDAG.py
COPY --chown=airflow:root conn.json /opt/airflow/backend_files/conn.json
COPY --chown=airflow:root airflow.cfg /opt/airflow/airflow.cfg
RUN pip install --no-cache-dir apache-airflow-providers-neo4j apache-airflow-providers-elasticsearch psycopg2
