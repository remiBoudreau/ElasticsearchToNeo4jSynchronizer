import os
from datetime import datetime, timedelta
import logging

import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook, ElasticsearchPythonHook
from airflow.models import DagRun

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = os.getenv("CYPHER_DAG","cypher_dag")

# import the logging module
import logging

# get the airflow.task logger
task_logger = logging.getLogger('airflow.task')

with DAG(
    DAG_ID,
    #schedule='@once',
    schedule_interval=None,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    start_date=datetime(2021, 1, 1),
    tags=['query','job','pipeline-controller','neo4j','elasticsearch'],
    catchup=False,
) as dag:
    dag.trigger_arguments = {"cypherQuery": "string"} # these are the arguments we would like to trigger manually
 
    @task(task_id='parse_job_args')
    def parse_job_args(ds=None, **kwargs):
        if kwargs['test_mode']:
            kwargs['dag_run'] = DagRun(conf=kwargs['params'])
        print(kwargs)
        print(ds)
        dag_run_conf = kwargs["dag_run"].conf #  here we get the parameters we specify when triggering
        print(dag_run_conf)
        kwargs["ti"].xcom_push(key="cypherQuery", value=dag_run_conf["cypherQuery"]) # push it as an airflow xcom
        return {"cypherQuery":dag_run_conf["cypherQuery"]}

    get_args = parse_job_args()

    @task(task_id='neo4j_query')
    def neo4j_query(ds=None, **kwargs):
        neo4j_hook = Neo4jHook(conn_id= 'neo4j_conn_id')
        cypher_query = kwargs["ti"].xcom_pull(key="cypherQuery")
        #cypher_query = kwargs["ti"].xcom_pull(task_ids='parse_job_args')["cypherQuery"]
        result = neo4j_hook.run(cypher_query)
        #neo4j_hook.run("MATCH(N) return N;")
        #return kwargs["ti"].xcom_pull(task_ids='parse_job_args')
        return result

    neo4j_task = neo4j_query()
    
    get_args >> neo4j_task

    @task(task_id='es_print_tables')
    def show_tables(ds=None, **kwargs):
        """
        show_tables queries elasticsearch to list available tables
        """
        # [START howto_elasticsearch_query]
        #es = ElasticsearchHook(elasticsearch_conn_id='es_conn_id')
        hook = ElasticsearchPythonHook([{'host': 'elasticsearch', 'port': '9200'}])
        result = kwargs["ti"].xcom_pull(task_ids='neo4j_query')

        # Handle ES conn with context manager

        #with hook.get_conn() as es_conn:
        es_conn = hook.get_conn
        client_id = 1
        correlation_id =2
            
        resp = es_conn.index(
                index='queries', 
                ignore=400, 
                doc_type='application/json', 
                id = correlation_id,
                body={"result":result})
            
        return resp
    
    execute_query = show_tables()
    
    neo4j_task >> execute_query