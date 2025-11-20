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
    dag_id='Producer_backfill_parallel',
    default_args=default_args,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    # --------------------------
    # UI Form 에 나타나는 Params
    # --------------------------
    # Airflow 3.x: Params는 여기에 선언
    params={
        "machines": Param(
            default="['machine-1-1']",
            description="['all'] 또는 ['machine-1-1', 'machine-1-8', 'machine-2-1', 'machine-2-9', 'machine-3-1', 'machine-3-11'] 형태로 입력"
        ),
        "topic": Param(
            default="backfill-topic",
            description="String type input"
        ),
        "partitions": Param(
            default="14",
            description="String type input"
        ),
        "replications": Param(
            default="1",
            description="String type input"
        )
    },
) as dag:

    # --------------------------------------------------------
    # (0) Params 기반 머신 리스트 정규화
    # --------------------------------------------------------
    @task
    def load_machine_list(machines):
        return machines     # ← 이 값이 XComArg(list) 가 됨
    
    @task
    def build_arguments_list(machines, topic, partitions, replications):
        """각 머신별 arguments 리스트를 생성"""
        return [
            [
                "--dest", "kafka",
                "--bootstrap-servers", "kafka.kafka.svc.cluster.local:9092",
                "--topic", str(topic),
                "--partitions", str(partitions),      # ← str() 추가
                "--replications", str(replications),  # ← str() 추가
                "--machine", machine,
            ]
            for machine in machines
        ]
    
    # 사용
    machines = load_machine_list("{{ params.machines }}")
    arguments_list = build_arguments_list(
        machines,
        "{{ params.topic }}",
        "{{ params.partitions }}",
        "{{ params.replications }}"
    )

    # (1) 머신별로 병렬 실행되는 Producer
    SMD_Producer_Backfill_Kafka = KubernetesPodOperator.partial(
        task_id="Producer_Backfill_Kafka",
        name="smd-producer-backfill-kafka",
        namespace="default",
        image="dwnusa/smd-producer-backfill:v0.1.2-amd64",
        cmds=[],
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
    ).expand(
        arguments=arguments_list  # ← XComArg가 아닌 리스트를 직접 전달
    )

    # 실행 순서: Backfill producer
    SMD_Producer_Backfill_Kafka