FROM apache/airflow:2.3.2-python3.10
ENV DEBIAN_FRONTEND=noninteractive
ENV AIRFLOW_UID=50000
ENV AIRFLOW_GID=0
COPY requirements.txt /requirements.txt
USER root
RUN apt update
USER 50000
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt