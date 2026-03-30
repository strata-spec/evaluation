from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import openai
import psycopg2

EVAL_MODEL = os.getenv("EVAL_MODEL", "deepseek-chat")
EVAL_BASE_URL = os.getenv("EVAL_BASE_URL", "https://api.deepseek.com")
EVAL_API_KEY = os.getenv("EVAL_API_KEY") or os.getenv("DEEPSEEK_API_KEY")

# Handle imports for both direct script execution and module import
try:
    from .scorer import (
        compute_metrics,
        compute_metrics_by_tier,
        execute_sql,
        load_questions,
        results_match,
        schema_link_score,
    )
    from .smif_loader import load_merged
    from .reporter import write_report
except ImportError:
    # When run directly as a script, use absolute imports
    sys.path.insert(0, str(Path(__file__).parent))
    from scorer import (
        compute_metrics,
        compute_metrics_by_tier,
        execute_sql,
        load_questions,
        results_match,
        schema_link_score,
    )
    from smif_loader import load_merged
    from reporter import write_report

BASELINE_SYSTEM = """\
You are a SQL expert. Given a natural language question, generate a single \
PostgreSQL SQL query that answers it correctly.
Return only the SQL query, no explanation, no markdown.

Question: {question}

Schema:
{schema}"""

STRATA_SYSTEM = """\
You are a SQL expert. Given a natural language question, generate a single \
PostgreSQL SQL query that answers it correctly.
Return only the SQL query, no explanation, no markdown.

Question: {question}

Semantic model:
{context}"""


def format_smif_context(model: dict, question: str) -> str:
    """
    Format the merged SMIF model as structured context for the agent prompt.
    Only include models relevant to the question.
    Relevance: model name or label appears (case-insensitive) in the question,
    OR the model has a column whose name or label appears in the question,
    OR there are fewer than 4 models total (include all).
    Always include relationships where both models are included.
    Format: domain description, then per-model blocks with columns and relationships.
    """
    if not isinstance(model, dict):
        return ""

    models = [item for item in model.get("models", []) or [] if isinstance(item, dict)]
    if not models:
        return ""

    lowered_question = question.casefold()
    include_all = len(models) < 4

    # Pass 1: seed set — keyword match or include-all threshold
    included_ids: set[str] = set()
    if include_all:
        included_ids = {
            item.get("model_id")
            for item in models
            if isinstance(item.get("model_id"), str)
        }
    else:
        for item in models:
            if _model_relevant(item, lowered_question):
                model_id = item.get("model_id")
                if isinstance(model_id, str):
                    included_ids.add(model_id)

    # Pass 2: relationship expansion — one hop, no recursion
    # For every seeded model, pull in the other endpoint of any relationship.
    all_relationships = [
        rel for rel in model.get("relationships", []) or [] if isinstance(rel, dict)
    ]
    # Snapshot pass-1 result so expansion does not cascade through newly added models.
    seed_snapshot = set(included_ids)
    for rel in all_relationships:
        from_m = rel.get("from_model")
        to_m = rel.get("to_model")
        if isinstance(from_m, str) and isinstance(to_m, str):
            if from_m in seed_snapshot:
                included_ids.add(to_m)
            if to_m in seed_snapshot:
                included_ids.add(from_m)

    # Fallback: include everything when seed + expansion produced nothing
    if not included_ids:
        included_ids = {
            item.get("model_id")
            for item in models
            if isinstance(item.get("model_id"), str)
        }

    # Preserve original model ordering
    included_models = [item for item in models if item.get("model_id") in included_ids]

    relationships = [
        rel
        for rel in all_relationships
        if rel.get("from_model") in included_ids
        and rel.get("to_model") in included_ids
    ]

    lines: list[str] = []
    domain = model.get("domain") if isinstance(model.get("domain"), dict) else {}
    domain_name = domain.get("name") or "Database"
    domain_description = domain.get("description") or ""
    lines.append(f"Database: {domain_name}")
    if domain_description:
        lines.append(str(domain_description))
    lines.append("")

    rels_by_from_model: dict[str, list[dict]] = {}
    for relationship in relationships:
        rels_by_from_model.setdefault(str(relationship.get("from_model")), []).append(relationship)

    for item in included_models:
        model_name = item.get("model_id") or item.get("name") or "unknown_model"
        table_name = (
            item.get("physical_source", {}).get("table")
            if isinstance(item.get("physical_source"), dict)
            else None
        ) or item.get("name") or model_name
        lines.append(f"Model: {model_name} (table: {table_name})")

        description = item.get("description") or ""
        grain = item.get("grain")
        if not grain and isinstance(item.get("x_properties"), dict):
            grain = item["x_properties"].get("grain")
        body_parts = [part for part in (description, grain) if part]
        if body_parts:
            lines.append(" ".join(str(part) for part in body_parts))

        lines.append("Columns:")
        columns = [col for col in item.get("columns", []) or [] if isinstance(col, dict)]
        if not columns:
            lines.append("  - none")
        else:
            for column in columns:
                column_name = column.get("name") or "unknown_column"
                role = column.get("role") or "unknown"
                description_text = column.get("description") or ""
                label = column.get("label")
                example_values = column.get("example_values") or []
                suffix = ""
                if label and label != column_name:
                    suffix = f" label: {label}."
                example_text = ""
                if example_values:
                    example_text = f" e.g. {example_values[0]}"
                detail = description_text if description_text else "No description."
                lines.append(f"  - {column_name} [{role}]: {detail}{suffix}{example_text}")

        model_relationships = rels_by_from_model.get(str(item.get("model_id")), [])
        if model_relationships:
            lines.append("")
            lines.append("Relationships:")
            for relationship in model_relationships:
                join = relationship.get('join_condition') or f"{relationship.get('from_model')}.{relationship.get('from_column')} = {relationship.get('to_model')}.{relationship.get('to_column')}"
                lines.append(
                    "  - "
                    f"{relationship.get('from_model')}.{relationship.get('from_column')} "
                    f"→ {relationship.get('to_model')}.{relationship.get('to_column')} "
                    f"({relationship.get('relationship_type') or 'unknown'}) "
                    f"on {join}"
                )

        lines.append("")

    return "\n".join(lines).strip()


def call_agent(
    openai_client,
    model: str,
    system_prompt: str,
) -> str | None:
    """
    Call the OpenAI-compatible API with the given system prompt.
    The user message is always: "Generate the SQL query."
    Returns the text response stripped of whitespace, or None on failure.
    Never raises.
    """
    try:
        response = openai_client.chat.completions.create(
            model=model,
            max_tokens=1024,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "Generate the SQL query."},
            ],
        )
        text = _extract_response_text(response)
        if not text:
            return None
        return _strip_sql_fences(text)
    except Exception:
        return None


def fetch_raw_schema(conn_str: str) -> str:
    """
    Query information_schema to build CREATE TABLE statements for all tables
    in the public schema. Include column names, types, and NOT NULL constraints.
    Include foreign key constraints as comments.
    Returns a single string. Never raises — returns empty string on failure.
    """
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(conn_str)
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT table_name, column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            """
        )
        columns = cursor.fetchall()
        if not columns:
            return ""

        cursor.execute(
            """
            SELECT
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                tc.constraint_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
            ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
            """
        )
        fk_rows = cursor.fetchall()

        columns_by_table: dict[str, list[str]] = {}
        for table_name, column_name, data_type, is_nullable, _ in columns:
            definition = f"  {column_name} {data_type}"
            if is_nullable == "NO":
                definition += " NOT NULL"
            columns_by_table.setdefault(str(table_name), []).append(definition)

        fk_comments_by_table: dict[str, list[str]] = {}
        for table_name, column_name, foreign_table_name, foreign_column_name, constraint_name in fk_rows:
            fk_comments_by_table.setdefault(str(table_name), []).append(
                f"-- FK {constraint_name}: {column_name} -> {foreign_table_name}.{foreign_column_name}"
            )

        statements: list[str] = []
        for table_name in sorted(columns_by_table):
            statements.append(f"CREATE TABLE {table_name} (")
            statements.append(",\n".join(columns_by_table[table_name]))
            statements.append(");")
            if table_name in fk_comments_by_table:
                statements.extend(fk_comments_by_table[table_name])
            statements.append("")

        return "\n".join(statements).strip()
    except Exception:
        return ""
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


def run_eval(
    db: str,
    semantic_path: str,
    corrections_path: str,
    questions_path: str,
    model: str,
    output_dir: str,
    tier: str | None = None,
    baseline_only: bool = False,
    strata_only: bool = False,
) -> list[dict]:
    """
    Run the full eval. Returns a list of per-question result dicts.
    Each dict matches the results.json schema in eval.md.
    Prints progress to stdout as it runs.
    """
    if baseline_only and strata_only:
        raise ValueError("--baseline-only and --strata-only cannot be used together")

    conditions: list[str]
    if baseline_only:
        conditions = ["baseline"]
    elif strata_only:
        conditions = ["strata"]
    else:
        conditions = ["baseline", "strata"]

    questions = load_questions(questions_path, tier=tier)
    schema = fetch_raw_schema(db) if "baseline" in conditions else ""
    merged_model = load_merged(semantic_path, corrections_path) if "strata" in conditions else {}

    api_key = EVAL_API_KEY
    if not api_key:
        raise ValueError(
            "An API key is required to run eval. "
            "Set EVAL_API_KEY or DEEPSEEK_API_KEY environment variable."
        )
    base_url = EVAL_BASE_URL
    client = openai.OpenAI(api_key=api_key, base_url=base_url)

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    for question in questions:
        reference_result = execute_sql(db, question["reference_sql"])
        prefix = f"{question['id']} [{question['tier']}]"
        if reference_result is None:
            print(f"{prefix} ✗ skipped — reference SQL execution failed")
            continue

        ref_cols, ref_rows = reference_result
        if len(ref_rows) == 0:
            print(f"{prefix} ✗ skipped — reference SQL returned zero rows")
            continue

        for condition in conditions:
            if condition == "baseline":
                system_prompt = BASELINE_SYSTEM.format(question=question["natural_language"], schema=schema)
            else:
                context = format_smif_context(merged_model, question["natural_language"])
                system_prompt = STRATA_SYSTEM.format(question=question["natural_language"], context=context)

            generated_sql = call_agent(client, model, system_prompt)
            execution_success = False
            result_match = False
            schema_link_correct = None

            if generated_sql is not None:
                generated_result = execute_sql(db, generated_sql)
                execution_success = generated_result is not None
                if generated_result is not None:
                    gen_cols, gen_rows = generated_result
                    result_match = results_match(ref_cols, ref_rows, gen_cols, gen_rows)
                schema_link_correct = schema_link_score(question["reference_sql"], generated_sql)

            status = "✓ match" if execution_success and result_match else "✗ exec error" if not execution_success else "✗ mismatch"
            print(f"{prefix} {condition:<8} ... {status}")

            results.append(
                {
                    "id": question["id"],
                    "tier": question["tier"],
                    "condition": condition,
                    "natural_language": question["natural_language"],
                    "generated_sql": generated_sql,
                    "reference_sql": question["reference_sql"],
                    "execution_success": execution_success,
                    "result_match": result_match,
                    "schema_link_correct": schema_link_correct,
                    "failure_mode": None,
                    "notes": question.get("notes", ""),
                }
            )

    _ = compute_metrics(results)
    _ = compute_metrics_by_tier(results)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Strata SQL eval.")
    parser.add_argument("--db", required=True, help="Postgres connection string")
    parser.add_argument("--semantic", default="../config/semantic.yaml", help="Path to semantic.yaml")
    parser.add_argument("--corrections", default="../config/corrections.yaml", help="Path to corrections.yaml")
    parser.add_argument("--questions", default="../questions/omdb.yaml", help="Path to questions YAML")
    parser.add_argument("--model", default=EVAL_MODEL, help="Model name (default: EVAL_MODEL env or deepseek-chat)")
    parser.add_argument("--output", default="../results/", help="Output directory for results JSON")
    parser.add_argument("--tier", help="Optional tier filter")
    parser.add_argument("--baseline-only", action="store_true", help="Run baseline condition only")
    parser.add_argument("--strata-only", action="store_true", help="Run strata condition only")
    args = parser.parse_args()

    if args.baseline_only and args.strata_only:
        print("error: --baseline-only and --strata-only cannot be used together", file=os.sys.stderr)
        return 1

    needs_semantic = not args.baseline_only
    if needs_semantic and not Path(args.semantic).exists():
        print(f"error: semantic model not found: {args.semantic}", file=os.sys.stderr)
        return 1

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        results = run_eval(
            db=args.db,
            semantic_path=args.semantic,
            corrections_path=args.corrections,
            questions_path=args.questions,
            model=args.model,
            output_dir=str(output_dir),
            tier=args.tier,
            baseline_only=args.baseline_only,
            strata_only=args.strata_only,
        )
    except Exception as exc:
        print(f"error: {exc}", file=os.sys.stderr)
        return 1

    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    results_path = output_dir / f"results_{run_id}.json"
    with results_path.open("w", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2)
        handle.write("\n")

    report_path = write_report(results, run_id, str(output_dir))

    question_count = len({item["id"] for item in results})
    condition_count = len({item["condition"] for item in results})
    model_source = "env" if args.model == EVAL_MODEL else "--model flag"
    print(f"model: {args.model} ({model_source})")
    print(f"base_url: {EVAL_BASE_URL}")
    print(f"Run complete: {question_count} questions × {condition_count} conditions = {len(results)} runs")
    print(f"Results written to {results_path}")
    print(f"Report written to {report_path}")
    return 0


def _model_relevant(model: dict, lowered_question: str) -> bool:
    names_to_check = [
        model.get("model_id"),
        model.get("name"),
        model.get("label"),
    ]
    for candidate in names_to_check:
        if isinstance(candidate, str) and candidate and candidate.casefold() in lowered_question:
            return True

    for column in model.get("columns", []) or []:
        if not isinstance(column, dict):
            continue
        for candidate in (column.get("name"), column.get("label")):
            if isinstance(candidate, str) and candidate and candidate.casefold() in lowered_question:
                return True
    return False


def _extract_response_text(response: Any) -> str:
    # OpenAI-compatible response: response.choices[0].message.content
    choices = getattr(response, "choices", None)
    if isinstance(choices, list) and choices:
        message = getattr(choices[0], "message", None)
        if message is not None:
            content = getattr(message, "content", None)
            if isinstance(content, str):
                return content.strip()
    # Fallback: plain string content
    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content.strip()
    return ""


def _strip_sql_fences(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```") and stripped.endswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 2:
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            return "\n".join(lines).strip()
    return stripped


if __name__ == "__main__":
    import sys
    sys.exit(main())
