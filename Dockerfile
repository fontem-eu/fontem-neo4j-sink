FROM python:3.12-slim

COPY void42-ca.crt /usr/local/share/ca-certificates/void42-ca.crt
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends ca-certificates \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENV PIP_INDEX_URL=https://nexus.void42.internal/repository/pypi-proxy/simple/ \
    PIP_TRUSTED_HOST=nexus.void42.internal

WORKDIR /app
COPY pyproject.toml .
COPY neo4j_sink/ ./neo4j_sink/

COPY vendor/gmr-events/      /tmp/gmr-events/
COPY vendor/gmr-event-schemas/ /tmp/gmr-event-schemas/
RUN pip install --no-cache-dir /tmp/gmr-event-schemas \
                                /tmp/gmr-events \
                                . \
 && rm -rf /tmp/gmr-events /tmp/gmr-event-schemas

RUN useradd --create-home --shell /bin/bash sink
USER sink

EXPOSE 9100
ENTRYPOINT ["python", "-m", "neo4j_sink"]
