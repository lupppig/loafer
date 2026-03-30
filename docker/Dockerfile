FROM python:3.11-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY loafer/ ./loafer/

ENTRYPOINT ["uv", "run", "loafer"]
