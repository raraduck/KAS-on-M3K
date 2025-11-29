from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
# from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 11),
}

with DAG(
    dag_id='Consumer_realtime_downstream',
    default_args=default_args,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    # --------------------------
    # UI Form 에 나타나는 Params
    # --------------------------
    # Airflow 3.x: Params는 여기에 선언
    params={
        "eph_topic": Param(
            default="realtime-monitor",
            description="Name of light-weight topic for batch-wise logging for spark streaming (Type None to disable)"
        ),
        "table": Param(
            default="datalake_table",
            description="Name of DB Table: dash(-) must be replaced to underline(_)"
        ),
        "executor": Param(
            default="2",
            # type="integer",
            description="Num of executors"
        ),
    },
) as dag:

    # (1) Spark Stream Upsert 실행
    Spark_Down_Stream_Upsert = SparkKubernetesOperator(
        task_id="Down_Stream_Upsert",
        name="spark-down-stream-upsert",
        in_cluster=True,              
        namespace="default",
        application_file="template-spark-stream-realtime-down.yaml",
        do_xcom_push=False,
        deferrable=False,  # 비동기 모드
        poll_interval=10,  # 상태 확인 간격
        get_logs=False  # 로그 수집 비활성화 (선택)
        # startup_timeout_seconds=120,  # 2분 안에 시작되면 OK
    )

    # 실행 순서: Spark stream upsert mode
    Spark_Down_Stream_Upsert