# Airflow plus this project's code. The slim image leaves out most providers, which
# keeps it small enough for a laptop.
FROM apache/airflow:slim-3.3.2-python3.12

COPY --chown=airflow:0 pyproject.toml README.md /opt/rain-watch/
COPY --chown=airflow:0 src /opt/rain-watch/src

# Pin Airflow so installing our package can't upgrade it, and add the Postgres driver
# Airflow needs for its own database.
RUN pip install --no-cache-dir "apache-airflow[postgres]==3.3.2" /opt/rain-watch \
 && python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"
