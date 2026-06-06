FROM python:3.14-slim

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

# Vendored wheels — pinned by filename. To bump fontem-events or
# fontem-event-schemas: build a new wheel in the producing repo,
# drop it into vendor/, delete the old one, and update the version
# pins in pyproject.toml. The pin + the wheel filename must agree;
# pip refuses to satisfy the pin from a wheel with a different
# version, so a mismatch fails the build.
COPY vendor/*.whl /tmp/wheels/
RUN pip install --no-cache-dir /tmp/wheels/*.whl . \
 && rm -rf /tmp/wheels

RUN useradd --create-home --shell /bin/bash sink
USER sink

EXPOSE 9100
ENTRYPOINT ["python", "-m", "neo4j_sink"]
