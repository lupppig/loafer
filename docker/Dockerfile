# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Build — install dependencies and build wheel
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy full source and build wheel
COPY . .
RUN uv build --wheel --out-dir dist/

# Create venv and install
RUN python -m venv /build/.venv \
    && /build/.venv/bin/pip install dist/*.whl

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2: Runtime — minimal image
# ──────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/lupppig/loafer"
LABEL org.opencontainers.image.description="AI-assisted ETL/ELT pipelines from the command line"

WORKDIR /app

# Copy the virtual environment from builder
COPY --from=builder /build/.venv /app/.venv

# Copy examples
COPY examples/ /app/examples/

# Create directories for user mounts
RUN mkdir -p /configs /data

# Add the venv bin to PATH so `loafer` is available directly
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Expose volume mount points
VOLUME ["/configs", "/data"]

# Default entrypoint
ENTRYPOINT ["loafer"]
CMD ["--help"]
