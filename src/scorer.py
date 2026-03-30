from __future__ import annotations

import argparse
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
import sqlglot
from psycopg2 import Error as PsycopgError
from sqlglot import exp
import yaml

__all__ = [
    "load_questions",
    "execute_sql",
    "normalise_result",
    "results_match",
    "schema_link_score",
    "compute_metrics",
    "compute_metrics_by_tier",
]

_VALID_TIERS = {"simple", "ambiguous", "multi_table", "metric", "gotcha"}
_REQUIRED_QUESTION_FIELDS = ("id", "tier", "natural_language", "reference_sql")


def load_questions(path: str, tier: str | None = None) -> list[dict]:
    """
    Load and validate questions from a YAML file.
    If tier is provided, return only questions of that tier.
    Raises ValueError if any question is missing required fields.
    Raises ValueError if reference_sql is present but empty.
    """
    if tier is not None and tier not in _VALID_TIERS:
        raise ValueError(f"Invalid tier: {tier}")

    data = _load_yaml_mapping(Path(path))
    raw_questions = data.get("questions")
    if not isinstance(raw_questions, list):
        raise ValueError("questions file must contain a 'questions' list")

    loaded: list[dict] = []
    for index, question in enumerate(raw_questions):
        if not isinstance(question, dict):
            raise ValueError(f"questions[{index}] must be a mapping")
        if question.get("skip") is True:
            continue

        _validate_question(question, index)
        if tier is None or question["tier"] == tier:
            loaded.append(dict(question))

    return loaded


def execute_sql(conn_str: str, sql: str) -> tuple[list[str], list[tuple]] | None:
    """
    Execute sql against Postgres. Returns (column_names, rows).
    Returns None if the query fails to execute (any exception).
    Never raises — execution failure is a valid result (QE metric).
    Connection is opened and closed per call. No connection pooling.
    """
    result, _ = _execute_sql_detailed(conn_str, sql)
    return result


def normalise_result(
    column_names: list[str],
    rows: list[tuple],
) -> list[dict]:
    """
    Normalise a result set for comparison.
    Returns a sorted list of dicts, one per row.
    Normalisation rules:
      - Column names: lowercased
      - String values: stripped and lowercased
      - Float/Decimal values: rounded to 2dp
      - None values: kept as None
      - Rows: sorted by their string representation after normalisation
    """
    lowered_columns = [name.lower() for name in column_names]
    normalised_rows: list[dict] = []

    for row in rows:
        normalised = {
            column_name: _normalise_value(value)
            for column_name, value in zip(lowered_columns, row, strict=False)
        }
        normalised_rows.append(normalised)

    normalised_rows.sort(key=lambda item: repr(sorted(item.items())))
    return normalised_rows


def results_match(
    ref_cols: list[str],
    ref_rows: list[tuple],
    gen_cols: list[str],
    gen_rows: list[tuple],
) -> bool:
    """
    Returns True if the two result sets are equivalent after normalisation.
    Column order is ignored. Row order is ignored.
    Column names must match (after lowercasing).
    """
    ref_index = _column_index_map(ref_cols)
    gen_index = _column_index_map(gen_cols)
    if set(ref_index) != set(gen_index):
        return False

    canonical_columns = sorted(ref_index)
    ref_reordered = _reorder_rows(ref_rows, ref_index, canonical_columns)
    gen_reordered = _reorder_rows(gen_rows, gen_index, canonical_columns)
    return normalise_result(canonical_columns, ref_reordered) == normalise_result(canonical_columns, gen_reordered)


def schema_link_score(reference_sql: str, generated_sql: str) -> bool | None:
    """
    Returns True if the generated SQL references the same tables and columns
    as the reference SQL.
    Uses sqlglot to extract table and column references from both.
    A question passes schema linking if:
      - All tables in reference_sql appear in generated_sql
      - No obviously wrong tables appear (tables in generated but not in reference)
    Column linking is best-effort: if sqlglot cannot parse either SQL, return None.
    Returns None on parse failure — callers must handle this.
    """
    try:
        reference_expr = sqlglot.parse_one(reference_sql, dialect="postgres")
        generated_expr = sqlglot.parse_one(generated_sql, dialect="postgres")
    except sqlglot.errors.ParseError:
        return None

    reference_tables = _extract_table_names(reference_expr)
    generated_tables = _extract_table_names(generated_expr)
    if reference_tables != generated_tables:
        return False

    reference_columns = _extract_column_names(reference_expr)
    generated_columns = _extract_column_names(generated_expr)
    if reference_columns and generated_columns and not reference_columns.issubset(generated_columns):
        return False

    return True


def compute_metrics(results: list[dict]) -> dict:
    """
    Given a list of per-question result dicts, compute EX, QE, SL.

    Each result dict has:
      - execution_success: bool  (False means QE failure)
      - result_match: bool       (True means EX pass; only meaningful if execution_success)
      - schema_link_correct: bool | None

    Returns:
      {
        "EX": float,   # fraction where execution_success and result_match
        "QE": float,   # fraction where not execution_success
        "SL": float,   # fraction where schema_link_correct is True
                       # (denominator: questions where SL is not None)
      }

    Returns zeros if results list is empty.
    """
    if not results:
        return {"EX": 0.0, "QE": 0.0, "SL": 0.0}

    total = len(results)
    ex_passes = sum(1 for item in results if item.get("execution_success") and item.get("result_match"))
    qe_failures = sum(1 for item in results if not item.get("execution_success"))
    sl_values = [item.get("schema_link_correct") for item in results if item.get("schema_link_correct") is not None]
    sl_passes = sum(1 for value in sl_values if value is True)

    return {
        "EX": ex_passes / total,
        "QE": qe_failures / total,
        "SL": sl_passes / len(sl_values) if sl_values else 0.0,
    }


def compute_metrics_by_tier(results: list[dict]) -> dict[str, dict]:
    """
    Same as compute_metrics but returns a dict keyed by tier.
    Each tier dict has EX, QE, SL plus a count of questions in that tier.
    """
    grouped: dict[str, list[dict]] = defaultdict(list)
    for item in results:
        tier = item.get("tier")
        if tier is None:
            continue
        grouped[str(tier)].append(item)

    output: dict[str, dict] = {}
    for tier_name in sorted(grouped):
        metrics = compute_metrics(grouped[tier_name])
        metrics["count"] = len(grouped[tier_name])
        output[tier_name] = metrics
    return output


def _validate_question(question: dict, index: int) -> None:
    missing = [field for field in _REQUIRED_QUESTION_FIELDS if field not in question]
    if missing:
        raise ValueError(f"questions[{index}] missing required fields: {', '.join(missing)}")

    reference_sql = question.get("reference_sql")
    if not isinstance(reference_sql, str) or not reference_sql.strip():
        raise ValueError(f"questions[{index}].reference_sql must be a non-empty string")

    tier = question.get("tier")
    if tier not in _VALID_TIERS:
        raise ValueError(f"questions[{index}].tier must be one of {', '.join(sorted(_VALID_TIERS))}")


def _load_yaml_mapping(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path.name} must parse to a mapping")
    return data


def _execute_sql_detailed(conn_str: str, sql: str) -> tuple[tuple[list[str], list[tuple]] | None, str | None]:
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(conn_str)
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description or []]
        return (column_names, rows), None
    except Exception as exc:
        return None, str(exc)
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except PsycopgError:
                pass
        if connection is not None:
            try:
                connection.close()
            except PsycopgError:
                pass


def _normalise_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip().lower()
    if isinstance(value, Decimal):
        return round(float(value), 2)
    if isinstance(value, float):
        return round(value, 2)
    return value


def _column_index_map(column_names: list[str]) -> dict[str, int]:
    return {column_name.lower(): index for index, column_name in enumerate(column_names)}


def _reorder_rows(rows: list[tuple], index_map: dict[str, int], canonical_columns: list[str]) -> list[tuple]:
    return [tuple(row[index_map[column_name]] for column_name in canonical_columns) for row in rows]


def _extract_table_names(expression: exp.Expression) -> set[str]:
    names: set[str] = set()
    for table in expression.find_all(exp.Table):
        if table.name:
            names.add(table.name.lower())
    return names


def _extract_column_names(expression: exp.Expression) -> set[str]:
    names: set[str] = set()
    for column in expression.find_all(exp.Column):
        if isinstance(column.this, exp.Star):
            continue
        if column.name:
            names.add(column.name.lower())
    return names


def _format_row_count(row_count: int) -> str:
    return f"{row_count} row" if row_count == 1 else f"{row_count} rows"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run reference SQL for Strata eval questions.")
    parser.add_argument("--db", required=True, help="Postgres connection string")
    parser.add_argument("--questions", required=True, help="Path to questions YAML file")
    parser.add_argument("--tier", choices=sorted(_VALID_TIERS), help="Optional single tier filter")
    args = parser.parse_args()

    questions = load_questions(args.questions, tier=args.tier)
    for question in questions:
        result, error = _execute_sql_detailed(args.db, question["reference_sql"])
        prefix = f"{question['id']} [{question['tier']}]"
        if result is None:
            print(f"{prefix} ✗ execution error: {error}")
            continue

        _, rows = result
        print(f"{prefix} ✓ {_format_row_count(len(rows))}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())