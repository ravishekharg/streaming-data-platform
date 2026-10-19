# Spark job image: official Spark 3.5 + Kafka and Iceberg jars baked in (no Maven download at pod start).
FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root
ARG ICEBERG=1.8.1
ARG SPARK=3.5.5
ARG KAFKA_CLIENTS=3.4.1
ARG M=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars && for j in \
      org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG}/iceberg-spark-runtime-3.5_2.12-${ICEBERG}.jar \
      org/apache/iceberg/iceberg-aws-bundle/${ICEBERG}/iceberg-aws-bundle-${ICEBERG}.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK}/spark-sql-kafka-0-10_2.12-${SPARK}.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK}/spark-token-provider-kafka-0-10_2.12-${SPARK}.jar \
      org/apache/kafka/kafka-clients/${KAFKA_CLIENTS}/kafka-clients-${KAFKA_CLIENTS}.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar ; do \
      wget -q "${M}/${j}" ; done && ls -1 /opt/spark/jars | grep -E 'iceberg|kafka|pool2|hadoop-aws|sdk-bundle'
# hadoop-aws (s3a://) is for the streaming checkpoint on S3; table data uses Iceberg's own S3FileIO.

WORKDIR /opt/sdp
COPY sdp/ sdp/
COPY jobs/ jobs/
ENV PYTHONPATH=/opt/sdp SDP_JARS_BUNDLED=1
USER spark
