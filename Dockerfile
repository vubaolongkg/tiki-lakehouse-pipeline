FROM apache/airflow:2.8.1-python3.10

USER root

# Cài đặt OpenJDK 17 và procps cho PySpark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt