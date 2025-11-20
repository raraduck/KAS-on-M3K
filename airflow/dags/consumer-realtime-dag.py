from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 11),
}

with DAG(
    dag_id='Consumer_realtime_stream',
    default_args=default_args,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "topic": Param(default="realtime-topic", description="Kafka topic name"),
        "table": Param(default="datalake_table", description="PostgreSQL table name")
    },
) as dag:

    # -----------------------------
    # Inline SparkApplication YAML
    # -----------------------------
    spark_manifest = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-stream-realtime
  namespace: default
spec:
  type: Python
  mode: cluster
  image: dwnusa/spark:v3.5.4.1-amd64
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///opt/spark-data/spark_stream_realtime.py
  sparkVersion: 3.5.4
  restartPolicy:
    type: Never
  arguments:
    - "--pg-host"
    - "10.246.246.33"
    - "--pg-port"
    - "12345"
    - "--pg-db"
    - "testdb"
    - "--pg-user"
    - "dwnusa"
    - "--pg-table"
    - "{{ params.table }}"
    - "--kafka-bootstrap"
    - "kafka.kafka.svc.cluster.local:9092"
    - "--topic"
    - "{{ params.topic }}"
    - "--trigger-interval"
    - "5 seconds"
    - "--checkpoint-location"
    - "/tmp/checkpoint"
  driver:
    cores: 1
    memory: 512m
    serviceAccount: spark-operator-spark
    volumeMounts:
      - name: data
        mountPath: /opt/spark-data
  executor:
    cores: 1
    instances: 4
    memory: 512m
    volumeMounts:
      - name: data
        mountPath: /opt/spark-data
  volumes:
    - name: data
      hostPath:
        path: /opt/spark/jobs
"""

    # Pod → SparkApplication 생성
    Spark_Stream_Upsert = KubernetesPodOperator(
        task_id="Stream_Upsert",
        name="spark-stream-upsert",
        namespace="default",
        image="bitnami/kubectl:latest",
        cmds=["/bin/sh", "-c"],
        arguments=[f'echo """{spark_manifest}""" | kubectl apply -f -'],
        in_cluster=True,
        get_logs=True,
        # is_delete_operator_pod=True,
        do_xcom_push=False
    )
