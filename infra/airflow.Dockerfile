FROM apache/airflow:3.4.0-python3.12@sha256:818f94affc5bd7ce0be0cc04f2ace09aa8382e65190a73a08881229ff32700cb
USER root
RUN python -m venv /opt/fleetpulse-venv && mkdir -p /opt/project && chown -R airflow:0 /opt/fleetpulse-venv /opt/project
USER airflow
COPY --chown=airflow:0 requirements-serving-lock.txt /opt/project/requirements-serving-lock.txt
RUN /opt/fleetpulse-venv/bin/pip install --no-user --no-cache-dir -r /opt/project/requirements-serving-lock.txt && /opt/fleetpulse-venv/bin/pip check
COPY --chown=airflow:0 requirements-lock.txt /opt/project/requirements-lock.txt
COPY --chown=airflow:0 ingestion /opt/project/ingestion
COPY --chown=airflow:0 transformations /opt/project/transformations
COPY --chown=airflow:0 ml /opt/project/ml
COPY --chown=airflow:0 serving /opt/project/serving
COPY --chown=airflow:0 orchestration /opt/project/orchestration
ENV PYTHONPATH=/opt/project
WORKDIR /opt/project
