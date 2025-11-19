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
    dag_id='backfill_pipeline_parallel',
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
            description="['all'] 또는 ['machine-1-1','machine-1-8','machine-2-1','machine-2-9', 'machine-3-1','machine-3-11'] 형태로 입력"
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
    # @task
    # def normalize_machine_list(machines):
    #     return machines

    # # Params 값은 Jinja로 전달
    # machine_list = normalize_machine_list("{{ params.machines }}")

    topic = dag.params["topic"]
    partitions = dag.params["partitions"]
    replications = dag.params["replications"]
    machines = dag.params["machines"]

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
        arguments=[
            [
                "--dest", "kafka",
                "--bootstrap-servers", "kafka.kafka.svc.cluster.local:9092",
                "--topic", topic,
                "--partitions", partitions,
                "--replications", replications,
                "--machine", m,
            ]
            for m in machines
        ]
    )

    # # (1) Backfill Producer 실행
    # SMD_Producer_Backfill_Kafka = KubernetesPodOperator(
    #     task_id="Producer_Backfill_Kafka",
    #     name="smd-producer-backfill-kafka",
    #     namespace="default",
    #     image="dwnusa/smd-producer-backfill:v0.1.2-amd64",
    #     cmds=[],   # 엔트리포인트 그대로 사용
    #     arguments=[
    #         [
    #             "--dest", "kafka",
    #             "--bootstrap-servers", "kafka.kafka.svc.cluster.local:9092",
    #             "--topic", "airflow-producer-backfill",
    #             "--partitions", "14",
    #             "--replications", "1",
    #             "--machine", machine_name,
    #         ]
    #         for machine_name in machines
    #     ],
    #     in_cluster=True,
    #     get_logs=True,
    #     is_delete_operator_pod=True,
    # )

    # (2) Spark Backfill Upsert 실행
    Spark_Backfill_Batch_Upsert = SparkKubernetesOperator(
        task_id="Spark_Backfill_Batch_Upsert",
        in_cluster=True,              
        namespace="default",
        application_file="{{ '/opt/spark-yaml/yaml/spark-batch-backfill-upsert.yaml' }}",  # ✅ Jinja 렌더링 무시
    )

    # 실행 순서: Backfill producer → Spark batch backfill upsert mode
    SMD_Producer_Backfill_Kafka >> Spark_Backfill_Batch_Upsert