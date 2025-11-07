FROM apache/airflow:3.0.2

USER root

# ──────────────────────────────────────────────
# 1️⃣ Java 설치 (Spark 실행용)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ──────────────────────────────────────────────
# 2️⃣ 환경변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# ──────────────────────────────────────────────
# 3️⃣ Spark 설치 (Spark-submit 전용 client)
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# ──────────────────────────────────────────────
# 4️⃣ Airflow 전용 Provider 설치
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ──────────────────────────────────────────────
# 5️⃣ 환경 확인
# RUN airflow version && java -version && spark-submit --version