FROM apache/superset:3.1.0

USER root

# SQLAlchemy compatible
RUN pip uninstall -y sqlalchemy && \
    pip install "sqlalchemy>=1.4,<2.0"

# CLICKHOUSE CONNECT (chuẩn Superset 3.x)
RUN pip install clickhouse-connect

USER superset