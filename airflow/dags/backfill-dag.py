from airflow import DAG
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
    dag_id='backfill_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    # -------------------------------------------------------------------
    # (0) UI 입력값 정규화
    #     - 입력 없으면 "all"
    #     - "all"이면 ["all"] 로 감싸서 하나의 Pod 만 생성되게 함
    # -------------------------------------------------------------------
    def normalize_machine_list(**context):
        machines = context["dag_run"].conf.get("machines", "all")

        # UI 입력이 문자열 "all"  → ["all"]
        if machines == "all":
            return ["all"]

        # UI 입력이 리스트 → 그대로 사용
        return machines

    NormalizeMachines = PythonOperator(
        task_id="NormalizeMachines",
        python_callable=normalize_machine_list,
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
        arguments=[
            [
                "--dest", "kafka",
                "--bootstrap-servers", "kafka.kafka.svc.cluster.local:9092",
                "--topic", "airflow-producer-backfill",
                "--partitions", "14",
                "--replications", "1",
                "--machine", machine_name,
            ]
            for machine_name in NormalizeMachines.output
        ],
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