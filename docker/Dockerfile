# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Build — install dependencies with uv, build wheel
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files and install runtime deps only
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Install the project itself
COPY loafer/ ./loafer/
RUN uv sync --frozen --no-dev

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2: Runtime — minimal image with only what's needed to run pipelines
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/lupppig/loafer"
LABEL org.opencontainers.image.description="AI-assisted ETL/ELT pipelines from the command line"

WORKDIR /app

# Copy the virtual environment from builder
COPY --from=builder /build/.venv /app/.venv

# Copy source code and examples
COPY loafer/ /app/loafer/
COPY examples/ /app/examples/

# Create directories for user mounts
RUN mkdir -p /configs /data /root/.loafer

# Add the venv bin to PATH so `loafer` is available directly
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Default environment variables (override with -e or .env file)
ENV GOOGLE_API_KEY=""

# Expose volume mount points
VOLUME ["/configs", "/data", "/root/.loafer"]

# Default entrypoint
ENTRYPOINT ["loafer"]
CMD ["--help"]
