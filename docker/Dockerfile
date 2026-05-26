# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Install into clean venv, then prune
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS installer

WORKDIR /install

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock README.md ./
COPY loafer/ ./loafer/

# Version is injected at build time so hatch-vcs doesn't need .git.
# setuptools-scm recommends the distribution-specific form (LOAFER_ETL is the
# normalized PEP 503 name for "loafer-etl"); we also set the generic var as a
# fallback. 0.0.0+unknown is a valid PEP 440 local version that makes an
# accidentally-unset build visibly unofficial rather than masquerading as 0.0.0.
ARG SETUPTOOLS_SCM_PRETEND_VERSION_FOR_LOAFER_ETL=0.0.0+unknown
ARG SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION_FOR_LOAFER_ETL}
ENV SETUPTOOLS_SCM_PRETEND_VERSION_FOR_LOAFER_ETL=${SETUPTOOLS_SCM_PRETEND_VERSION_FOR_LOAFER_ETL}
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

RUN uv venv /app/.venv \
    && VIRTUAL_ENV=/app/.venv \
       SETUPTOOLS_SCM_PRETEND_VERSION_FOR_LOAFER_ETL=${SETUPTOOLS_SCM_PRETEND_VERSION_FOR_LOAFER_ETL} \
       SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION} \
       uv pip install --no-cache .

# Best-effort cleanup (separate step so install failures are never masked)
RUN find /app/.venv -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; \
    find /app/.venv -type f -name '*.pyc' -delete 2>/dev/null; \
    find /app/.venv -type d \( -name tests -o -name test -o -name doc -o -name docs \) -exec rm -rf {} + 2>/dev/null; \
    find /app/.venv -name '*.so' -exec strip --strip-unneeded {} + 2>/dev/null; \
    exit 0

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
