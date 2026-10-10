
FROM apache/spark:4.0.4-java21-python3

USER root

RUN python3 -m pip install \
    --no-cache-dir \
    "cassandra-driver>=3.29,<4"

RUN python3 -c "from cassandra.cluster import Cluster; print('CASSANDRA DRIVER OK')"

USER 185
