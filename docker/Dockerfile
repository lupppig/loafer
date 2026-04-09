# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Install into clean venv, then prune
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS installer

WORKDIR /install

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock README.md ./
COPY loafer/ ./loafer/
COPY .git/ ./.git/

RUN set -e; \
    uv venv /app/.venv; \
    VIRTUAL_ENV=/app/.venv uv pip install --no-cache .; \
    find /app/.venv -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true; \
    find /app/.venv -type f -name '*.pyc' -delete 2>/dev/null || true; \
    find /app/.venv -type d -name tests -exec rm -rf {} + 2>/dev/null || true; \
    find /app/.venv -type d -name test -exec rm -rf {} + 2>/dev/null || true; \
    find /app/.venv -type d -name doc -exec rm -rf {} + 2>/dev/null || true; \
    find /app/.venv -type d -name docs -exec rm -rf {} + 2>/dev/null || true; \
    find /app/.venv -name '*.so' -exec strip --strip-unneeded {} + 2>/dev/null || true

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2: Runtime — minimal image
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/lupppig/loafer"
LABEL org.opencontainers.image.description="AI-assisted ETL/ELT pipelines from the command line"

WORKDIR /app

# Copy pruned venv
COPY --from=installer /app/.venv /app/.venv

# Copy examples
COPY examples/ /app/examples/

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["loafer"]
CMD ["--help"]
