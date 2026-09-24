"""
Dump the public app's OpenAPI schema to stdout, offline.

    .venv/bin/python scripts/export_openapi.py > openapi.json

This is `main_public.app`: the routes a browser at data.courtvision.dev can
reach, which is the surface the dashboard's generated types describe. The
public app serves no /openapi.json (`openapi_url=None`), so this script is the
only way to get its schema.

No server and no database: `main_public.py` does its DB work in the lifespan
handler, which never runs here. Importing the app is enough to build the
schema. The env vars below only need to *exist* for settings to import; their
values are never used.

Output is sorted and indented so diffs are stable.
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost:5432/x")
os.environ.setdefault("PIPELINE_API_TOKEN", "export-only")

from main_public import app  # noqa: E402

json.dump(app.openapi(), sys.stdout, indent=2, sort_keys=True)
sys.stdout.write("\n")
