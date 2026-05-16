FROM flink:1.18-scala_2.12-java11

# ─── System dependencies ──────────────────────────────────────────────────────
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python3 python3-pip python3-dev \
        build-essential wget curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# ─── Flink JAR connectors ─────────────────────────────────────────────────────
# Only Kafka is needed as a Java JAR; Cassandra and Redis will use pure Python sinks.
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# ─── Python / PyFlink dependencies ───────────────────────────────────────────
RUN python3 -m pip install --upgrade --no-cache-dir pip setuptools wheel

COPY requirements.txt /opt/flink/usrlib/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/usrlib/requirements.txt

# ─── spaCy model (pinned wheel) ──────────────────────────────────────────────
RUN pip3 install --no-cache-dir \
    https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.7.1/en_core_web_sm-3.7.1-py3-none-any.whl

# ─── Flink job source files ───────────────────────────────────────────────────
COPY . /opt/flink/usrlib/

WORKDIR /opt/flink