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
    dag_id='spark_kubernetes_example',
    default_args=default_args,
    schedule=None,
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
        application_file="/opt/spark/yaml/spark-consume.yaml", #"spark-consume.yaml",  
    )

    # (3) NVIDIA SMI Pod 실행
    nvidia_smi_check = KubernetesPodOperator(
        task_id="nvidia_smi_check",
        name="nvidia-smi",
        namespace="default",
        image="nvidia/cuda:12.2.0-base-ubuntu22.04",
        cmds=["nvidia-smi"],
        resources={"limit_gpu": 1},   # GPU 1개 요청
        get_logs=True,
        is_delete_operator_pod=True,  # 실행 후 Pod 삭제
        in_cluster=True
    )

    # 실행 순서: hello → spark → GPU 체크
    hello_task >> spark_submit >> nvidia_smi_check