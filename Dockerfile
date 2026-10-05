# ── build: venv + void42 CA + local package/vendored wheels ───────────────────
FROM cgr.void42.internal/chainguard/python:latest-dev@sha256:82943d7c508865fa97d15d129d524991100e66a8f89450233704d4ea70accd95 AS build
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
# The runtime needs the packages in the venv, not the tool that installed
# them: pip in a runtime image fetches and installs code (docker-build-sign
# checks runtime images for it).
RUN pip uninstall -y pip

# ── runtime: distroless; neo4j_sink installed into the venv ───────────────────
FROM cgr.void42.internal/chainguard/python:latest@sha256:1961420e5f93bd056d4b0b40eca12cdf01b3ed09177aa4d6ec71fab38cbf158f
WORKDIR /app
COPY --from=build /venv /venv
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENV PATH="/venv/bin:$PATH" \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
USER 65532
EXPOSE 9100
ENTRYPOINT ["/venv/bin/python", "-m", "neo4j_sink"]
