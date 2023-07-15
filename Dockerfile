FROM openjdk:8-jre

# Set environment variables
ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=3.2
ENV SPARK_HOME=/spark

# Install Python and dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip

COPY requirements.txt /home
WORKDIR /home
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Download and install Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set Spark environment variables
ENV PATH=${SPARK_HOME}/bin:${PATH}
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip

# Set the entrypoint to start PySpark
# ENTRYPOINT ["pyspark"]
CMD ["tail", "-f", "/dev/null"]
