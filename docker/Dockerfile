# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Install the locked production environment
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11.15-slim-trixie@sha256:db3ff2e1800a8581e2c48a27c3995339d47bdf046da21c7627accd3d51053a93 AS installer

WORKDIR /install

COPY --from=ghcr.io/astral-sh/uv:0.11.16@sha256:440fd6477af86a2f1b38080c539f1672cd22acb1b1a47e321dba5158ab08864d /uv /usr/local/bin/uv

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
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT=/app/.venv

RUN uv sync --locked --no-dev --no-editable --no-cache

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2: Runtime — minimal image
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11.15-slim-trixie@sha256:db3ff2e1800a8581e2c48a27c3995339d47bdf046da21c7627accd3d51053a93 AS runtime

LABEL org.opencontainers.image.source="https://github.com/lupppig/loafer"
LABEL org.opencontainers.image.description="AI-assisted ETL/ELT pipelines from the command line"

RUN python -m pip uninstall --yes setuptools wheel pip \
    && groupadd --gid 10001 loafer \
    && useradd --uid 10001 --gid loafer --home-dir /home/loafer --create-home \
       --shell /usr/sbin/nologin loafer \
    && mkdir -p /home/loafer/.loafer/objects /workspace \
    && chown -R loafer:loafer /home/loafer /workspace

COPY --from=installer /app/.venv /app/.venv
COPY examples/ /app/examples/

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV HOME=/home/loafer

WORKDIR /workspace
USER 10001:10001

ENTRYPOINT ["loafer"]
CMD ["--help"]
