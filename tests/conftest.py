import os


# Ensure required env vars exist before importing app modules.
os.environ.setdefault("DATABASE_URL", "postgresql://cv:cv@localhost:5432/cv_test")
os.environ.setdefault("PIPELINE_API_TOKEN", "test-token")

