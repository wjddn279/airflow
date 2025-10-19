FROM apache/airflow:3.1.0


# root로 복사 후 UID/GID로 변경 (일반적으로 airflow는 50000)

USER root

RUN mv /home/airflow/.local/lib/python3.12/site-packages/airflow/providers /home/airflow/
RUN rm -rf /home/airflow/.local/lib/python3.12/site-packages/airflow
COPY ./src/airflow/ /home/airflow/.local/lib/python3.12/site-packages/airflow/

RUN mv /home/airflow/providers/ /home/airflow/.local/lib/python3.12/site-packages/airflow/providers

USER airflow

# RUN #pip install pygtrie && pip install natsort

EXPOSE 8080
