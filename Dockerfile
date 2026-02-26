FROM python:3.12-slim-bookworm

# 1. Set environment variables
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing pyc files to disc
# PYTHONUNBUFFERED: Ensures logs are flushed immediately to the stream
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 2. Install system dependencies required for building Python packages
# - gcc & build-essential: Required to compile psycopg2 and other C-extensions
# - libpq-dev: Required header files for PostgreSQL (psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 3. Copy project files and install Python dependencies
COPY pyproject.toml .
COPY requirements.txt .
COPY db/ ./db/
COPY tasks/ ./tasks/
COPY static/ ./static/
COPY utils/ ./utils/
COPY core/ ./core/
COPY schemas/ ./schemas/
COPY api/ ./api/
COPY pipelines/ ./pipelines/
COPY services/ ./services/

# Install Python dependencies from requirements.txt and the local package
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -e .

# 4. Copy remaining application code
COPY main.py .

# 5. Create a non-root user for security
# Running as root is a security risk. We create a user 'appuser' and switch to it.
RUN useradd -m -u 1000 appuser
USER appuser

EXPOSE 8001

CMD ["uvicorn", "main:app", "--host", "::", "--port", "8001"]
