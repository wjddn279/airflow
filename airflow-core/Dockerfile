FROM apache/airflow:3.1.2rc1


# root로 복사 후 UID/GID로 변경 (일반적으로 airflow는 50000)

USER root

RUN apt-get update -y && apt-get install procps -y && apt-get install smem -y

RUN mv /home/airflow/.local/lib/python3.12/site-packages/airflow/providers /home/airflow/
RUN rm -rf /home/airflow/.local/lib/python3.12/site-packages/airflow
COPY ./src/airflow/ /home/airflow/.local/lib/python3.12/site-packages/airflow/

RUN mv /home/airflow/providers/ /home/airflow/.local/lib/python3.12/site-packages/airflow/providers

USER airflow

RUN pip install memray

EXPOSE 8080
