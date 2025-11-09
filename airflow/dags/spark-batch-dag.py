from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
# from kubernetes.client import models as k8s

def say_hello():
    print("Hello from Airflow DAG using SparkKubernetesOperator!")
    return "DAG executed successfully."

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 11),
}

with DAG(
    dag_id='spark_batch_daily',
    default_args=default_args,
    schedule='@daily',   # ← 매일 1회 실행
    catchup=False,
) as dag:

    # (1) 단순 파이썬 Task
    hello_task = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )

    # (2) Spark Application 생성
    spark_submit = SparkKubernetesOperator(
        task_id="submit_spark_application",
        in_cluster=True,              
        namespace="default",
        application_file="{{ '/opt/spark-yaml/yaml/spark-batch-job.yaml' }}",  # ✅ Jinja 렌더링 무시
    )

    # 실행 순서: hello → spark → GPU 체크
    hello_task >> spark_submit