# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Build — create wheel
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

ARG APP_VERSION

WORKDIR /build

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock README.md ./
COPY loafer/ ./loafer/

# hatch-vcs needs either .git or SETUPTOOLS_SCM_PRETEND_VERSION
ENV SETUPTOOLS_SCM_PRETEND_VERSION_FOR_LOAFER_ETL=${APP_VERSION}
RUN uv build --wheel --out-dir dist/

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2: Install into clean venv, then prune
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS installer

WORKDIR /install

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY --from=builder /build/dist/*.whl ./

RUN python -m venv /install/.venv \
    && /install/.venv/bin/pip install --no-cache-dir *.whl \
    # Remove .pyc files and __pycache__
    && find /install/.venv -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true \
    && find /install/.venv -type f -name '*.pyc' -delete 2>/dev/null || true \
    # Remove test and doc directories
    && find /install/.venv -type d -name tests -exec rm -rf {} + 2>/dev/null || true \
    && find /install/.venv -type d -name test -exec rm -rf {} + 2>/dev/null || true \
    && find /install/.venv -type d -name doc -exec rm -rf {} + 2>/dev/null || true \
    && find /install/.venv -type d -name docs -exec rm -rf {} + 2>/dev/null || true \
    # Remove pip cache
    && rm -rf /root/.cache/pip \
    # Strip debug symbols from shared libraries
    && find /install/.venv -name '*.so' -exec strip --strip-unneeded {} + 2>/dev/null || true

# ──────────────────────────────────────────────────────────────────────────────
# Stage 3: Runtime — minimal image
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/lupppig/loafer"
LABEL org.opencontainers.image.description="AI-assisted ETL/ELT pipelines from the command line"

WORKDIR /app

# Copy pruned venv
COPY --from=installer /install/.venv /app/.venv

# Copy examples
COPY examples/ /app/examples/

# Create directories for user mounts
RUN mkdir -p /configs /data

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

VOLUME ["/configs", "/data"]

ENTRYPOINT ["loafer"]
CMD ["--help"]
