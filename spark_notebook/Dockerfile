FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

ENV SPARK_CONF_DIR=/opt/spark/conf

RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.540/aws-java-sdk-bundle-1.12.540.jar -P ${SPARK_HOME}/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P ${SPARK_HOME}/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar -P ${SPARK_HOME}/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar -P ${SPARK_HOME}/jars/
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar -P ${SPARK_HOME}/jars/

RUN chown -R ${NB_UID}:${NB_GID} ${SPARK_HOME}/jars

COPY spark_notebook/spark-defaults.conf $SPARK_CONF_DIR/
RUN chown -R ${NB_UID}:${NB_GID} $SPARK_CONF_DIR

USER $NB_USER

WORKDIR /app
