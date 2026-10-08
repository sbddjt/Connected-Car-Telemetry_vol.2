# syntax=docker/dockerfile:1
FROM python:3.13-slim-bookworm AS server
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --gid 10001 telemetry \
    && useradd --uid 10001 --gid telemetry --create-home telemetry \
    && mkdir -p /app/data && chown telemetry:telemetry /app/data
COPY --chown=telemetry:telemetry src/ ./src/
COPY --chown=telemetry:telemetry config/*.json ./config/
COPY --chown=telemetry:telemetry frontend/ ./frontend/
USER 10001:10001
EXPOSE 8765 8080
ENTRYPOINT ["python", "src/container_entrypoint.py"]
CMD ["query-api"]

FROM server AS simulator
ARG TARGETARCH
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && case "$TARGETARCH" in amd64) platform=manylinux2014_x86_64 ;; arm64) platform=manylinux2014_aarch64 ;; *) exit 1 ;; esac \
    && pip download --only-binary=:all: --no-deps --platform "$platform" --dest /tmp/sumo eclipse-sumo==1.27.1 \
    && pip install --no-cache-dir /tmp/sumo/*.whl \
    && rm -rf /tmp/sumo
RUN apt-get update && apt-get install -y --no-install-recommends libx11-6 libxext6 libxrender1 libgl1 \
    && rm -rf /var/lib/apt/lists/* \
    && sumo --version
COPY --chown=telemetry:telemetry scenario/gangnam_expanded/*.gz ./scenario/gangnam_expanded/
COPY --chown=telemetry:telemetry config/gangnam_expanded_sumo.sumocfg ./config/
ENV VEHICLE_SUMO_BINARY=sumo SUMO_HOME=/usr/local/lib/python3.13/site-packages/sumo
USER 10001:10001
CMD ["vehicle-collector"]
