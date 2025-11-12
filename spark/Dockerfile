FROM docker.io/library/spark:3.5.4

# ✅ 루트 권한으로 변경
USER root

WORKDIR /opt/spark-data

# ✅ 패키지 설치 (root 권한으로 설치 가능)
RUN pip install --no-cache-dir python-dotenv pyspark pandas numpy matplotlib scikit-learn kafka-python

# ✅ Spark jars 복사
COPY jars /opt/spark/jars/

# ✅ 다시 spark 사용자로 복귀 (보안·권한 유지)
USER spark