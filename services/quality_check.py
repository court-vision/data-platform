"""
The shape of one data-quality check.

Its own module so the check catalogues (`services/consistency_checks.py`, and
the structural and timing ones in `services/data_quality_service.py`) can
share it without importing each other.
"""

from __future__ import annotations

from dataclasses import dataclass

# How many offending rows a failed check keeps with its result.
SAMPLE_LIMIT = 5


@dataclass(frozen=True)
class SQLQualityCheck:
    name: str
    severity: str
    # One scalar: how many rows break the rule. 0 passes.
    sql: str
    failure_message: str
    # What the dashboard's quality pages say about a check beyond its result:
    # the table it guards ("schema.table"), which group it belongs to, and for
    # a timing check the pipeline whose runs it watches.
    table: str = ""
    group: str = "structural"  # structural | consistency | timing
    pipeline: str | None = None
    # A consistency check holds `table` to account against these.
    against: tuple[str, ...] = ()
    # The offending rows themselves, run only once `sql` has counted a failure.
    # A count says something is wrong; this says which player, which night.
    sample_sql: str | None = None
