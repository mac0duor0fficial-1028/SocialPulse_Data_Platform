FROM apache/airflow:2.10.5-python3.11

USER airflow
COPY requirements.txt /tmp/requirements.txt
# Airflow's constraint file keeps its own dependency set consistent; our pinned libraries are
# installed on top (pandas/pyarrow/boto3 versions are compatible with these constraints).
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ENV PYTHONPATH=/opt/airflow/project
