"""Consistency checks between JOB_COLS, _INSERT_SQL, Job, and JobInsert."""
from __future__ import annotations

import re
import dataclasses

from wrk.repos import _INSERT_SQL, _INSERT_COLS
from wrk.schemas import Job, JobInsert, JOB_COLS


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_col_list(text: str) -> list[str]:
    return [c.strip() for c in text.split(",") if c.strip()]


def _extract_insert_cols(sql: str) -> list[str]:
    """Column names from the INSERT INTO {jobs} (...) section."""
    start = sql.index("INSERT INTO {jobs}")
    open_p = sql.index("(", start)
    close_p = sql.index(")", open_p)
    return _parse_col_list(sql[open_p + 1 : close_p])


def _extract_values_section(sql: str) -> str:
    """Body of the VALUES (...) clause, handling nested parens."""
    values_idx = sql.upper().index("VALUES")
    open_p = sql.index("(", values_idx)
    depth = 0
    for i, ch in enumerate(sql[open_p:], open_p):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return sql[open_p + 1 : i]
    raise ValueError("Unbalanced parentheses in VALUES clause")


def _extract_placeholders(sql: str) -> set[str]:
    """%(name)s placeholder names from the VALUES body."""
    return set(re.findall(r"%\((\w+)\)s", _extract_values_section(sql)))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestJobColsMatchesJobDataclass:
    def test_every_col_is_a_field(self):
        """Each name in JOB_COLS must correspond to a Job field."""
        cols = set(_parse_col_list(JOB_COLS))
        fields = {f.name for f in dataclasses.fields(Job)}
        extra = cols - fields
        assert not extra, f"In JOB_COLS but not Job: {extra}"

    def test_every_field_is_in_col_list(self):
        """Each Job field must be listed in JOB_COLS (so from_row covers it)."""
        cols = set(_parse_col_list(JOB_COLS))
        fields = {f.name for f in dataclasses.fields(Job)}
        missing = fields - cols
        assert not missing, f"In Job but not JOB_COLS: {missing}"


class TestInsertSqlConsistency:
    def test_insert_cols_are_in_job_cols(self):
        """Every column named in the INSERT must appear in JOB_COLS."""
        job_cols = set(_parse_col_list(JOB_COLS))
        insert_cols = set(_extract_insert_cols(_INSERT_SQL))
        extra = insert_cols - job_cols
        assert not extra, f"INSERT columns missing from JOB_COLS: {extra}"

    def test_column_count_matches_placeholder_count(self):
        """Number of INSERT columns must equal number of VALUES placeholders."""
        cols = _extract_insert_cols(_INSERT_SQL)
        placeholders = _extract_placeholders(_INSERT_SQL)
        assert len(cols) == len(placeholders), (
            f"INSERT has {len(cols)} columns but VALUES has {len(placeholders)} placeholders"
        )

    def test_insert_cols_exclude_dep_ids(self):
        """dep_ids is not a DB column and must not appear in the INSERT."""
        assert "dep_ids" not in _INSERT_COLS

    def test_as_params_keys_match_insert_cols(self):
        """JobInsert.as_params() must cover exactly the INSERT column set."""
        params_keys = set(
            JobInsert(
                function="fn",
                queue="default",
                status="queued",
                priority=0,
                max_attempts=1,
                failure_mode="hold",
            ).as_params()
        )
        assert params_keys == set(_INSERT_COLS)
