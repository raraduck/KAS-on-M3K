FROM bitnamilegacy/spark:3.5.4

# S3A 관련 JAR 추가 (Hadoop AWS + AWS SDK Bundle)
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P /opt/bitnami/spark/jars/

# (기존 JAR 복사 유지)
COPY ./jars /opt/bitnami/spark/jars/