# ---------- Builder ----------
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /project

# 1. Copy dependency files
COPY pyproject.toml ./
COPY uv.lock* ./

# 2. Explicitly set up a venv path for uv
ENV UV_PROJECT_ENVIRONMENT=/project/.venv

# 3. Install dependencies into that venv
RUN uv sync --frozen --no-cache

# 4. Copy your app code
COPY logging_config.py .
COPY db_work_order_image_extract.py .

# 5. Pre-create output dirs
RUN mkdir -p /project/output/json1 /project/output/images

# ---------- Runtime ----------
FROM python:3.13-slim-bookworm AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends libmariadb3 && \
    rm -rf /var/lib/apt/lists/*

# Non-root user
RUN adduser --disabled-password --gecos '' appuser

# Copy the virtual environment + app code
COPY --from=builder --chown=appuser:appuser /project/.venv   /opt/venv
COPY --from=builder --chown=appuser:appuser /project/*.py   /app/
COPY --from=builder --chown=appuser:appuser /project/output /app/output

# Runtime env
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
USER appuser

CMD ["/opt/venv/bin/python", "db_work_order_image_extract.py"]
