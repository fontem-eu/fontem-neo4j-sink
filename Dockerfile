# ── build: venv + void42 CA + local package/vendored wheels ───────────────────
FROM cgr.void42.internal/chainguard/python:latest-dev@sha256:87729167739190d9309588120a8ac0ffbf2eb95dd895abb9bada6bb9ecbf5a04 AS build
USER root
ENV PIP_INDEX_URL=https://nexus.void42.internal/repository/pypi-proxy/simple/ \
    PIP_TRUSTED_HOST=nexus.void42.internal
COPY void42-ca.crt /tmp/void42-ca.crt
RUN cat /tmp/void42-ca.crt >> /etc/ssl/certs/ca-certificates.crt
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"
WORKDIR /build
COPY pyproject.toml .
COPY neo4j_sink/ ./neo4j_sink/
COPY vendor/*.whl /tmp/wheels/
RUN pip install --no-cache-dir /tmp/wheels/*.whl .

# ── runtime: distroless; neo4j_sink installed into the venv ───────────────────
FROM cgr.void42.internal/chainguard/python:latest@sha256:be59f7abb600892c78e6ff0fe1a8f39549da13302dc003c8a5eb1c14b03d995c
WORKDIR /app
COPY --from=build /venv /venv
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENV PATH="/venv/bin:$PATH" \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
USER 65532
EXPOSE 9100
ENTRYPOINT ["/venv/bin/python", "-m", "neo4j_sink"]
