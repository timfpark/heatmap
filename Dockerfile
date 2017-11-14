FROM kubespark/spark-executor-py:v2.2.0-kubernetes-0.5.0

RUN mkdir -p /opt/spark/code

COPY spark-cassandra-connector-2.0.5-s_2.11.jar /opt/spark/code
COPY heatmap.py /opt/spark/code
COPY tile.py /opt/spark/code
