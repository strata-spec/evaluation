"""
Reporter module for Strata eval.
Reads results.json, classifies failure modes, writes report.md.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def classify_failure(result: dict) -> str | None:
    """
    Classify the failure mode of a result dict.
    Returns None if result_match is True (not a failure).

    Classification rules (in priority order):
    1. execution_success is False → "schema hallucination"
    2. schema_link_correct is False → "wrong table or column"
    3. schema_link_correct is True, result_match is False → "logic error"
    4. schema_link_correct is None, result_match is False → "logic or schema error"

    Args:
        result: Per-question result dict from results.json

    Returns:
        Failure mode string or None (if not a failure)
    """
    # If result matched, no failure
    if result.get("result_match"):
        return None

    # Priority 1: execution failure → schema hallucination
    if not result.get("execution_success"):
        return "schema hallucination"

    # Priority 2: schema linking failed
    if result.get("schema_link_correct") is False:
        return "wrong table or column"

    # Priority 3: schema linking succeeded but result mismatch → logic error
    if result.get("schema_link_correct") is True:
        return "logic error"

    # Priority 4: schema linking parse failed → needs manual review
    if result.get("schema_link_correct") is None:
        return "logic or schema error"

    return None


def generate_report(results: list[dict], run_id: str) -> str:
    """
    Generate the full report.md content as a string.
    Classifies failures, generates summary tables, failure blocks, patterns.

    Args:
        results: List of per-question result dicts from results.json
        run_id: Timestamp identifier for the run

    Returns:
        Full markdown report as a string
    """
    # Group results by (id, condition) for easy lookup
    results_by_id_condition: dict[tuple[str, str], dict[str, Any]] = {}
    for result in results:
        key = (result["id"], result["condition"])
        results_by_id_condition[key] = result

    # Get unique questions in order they appear
    seen_ids: set[str] = set()
    unique_questions: list[str] = []
    for result in results:
        if result["id"] not in seen_ids:
            unique_questions.append(result["id"])
            seen_ids.add(result["id"])

    # Get unique tiers in order
    seen_tiers: set[str] = set()
    unique_tiers: list[str] = []
    for result in results:
        if result["tier"] not in seen_tiers:
            unique_tiers.append(result["tier"])
            seen_tiers.add(result["tier"])

    # Get unique conditions
    unique_conditions: list[str] = []
    seen_conditions: set[str] = set()
    for result in results:
        if result["condition"] not in seen_conditions:
            unique_conditions.append(result["condition"])
            seen_conditions.add(result["condition"])

    # Classify all failures
    classified: list[dict[str, Any]] = []
    for result in results:
        failure_mode = classify_failure(result)
        classified.append(
            {
                **result,
                "failure_mode": failure_mode,
            }
        )

    # Compute metrics
    def is_success(r: dict) -> bool:
        return r.get("execution_success") and r.get("result_match")

    def count_by_tier_condition(tier: str, condition: str, predicate) -> int:
        return sum(
            1
            for r in classified
            if r.get("tier") == tier and r.get("condition") == condition and predicate(r)
        )

    def total_by_condition(condition: str, predicate) -> int:
        return sum(1 for r in classified if r.get("condition") == condition and predicate(r))

    # Build summary table
    summary_lines: list[str] = []
    summary_lines.append(f"# Strata eval — {run_id}\n")
    summary_lines.append("## Summary\n")
    summary_lines.append("| Metric | Baseline | Strata | Delta |")
    summary_lines.append("|--------|----------|--------|-------|")

    # Only include metrics for conditions that were run
    if "baseline" in unique_conditions and "strata" in unique_conditions:
        # Both conditions: compute full metrics
        baseline_ex = (
            total_by_condition("baseline", is_success)
            / len([r for r in classified if r["condition"] == "baseline"])
            * 100
            if any(r["condition"] == "baseline" for r in classified)
            else 0
        )
        strata_ex = (
            total_by_condition("strata", is_success)
            / len([r for r in classified if r["condition"] == "strata"])
            * 100
            if any(r["condition"] == "strata" for r in classified)
            else 0
        )
        delta_ex = strata_ex - baseline_ex

        # QE: query error rate (failures / total)
        baseline_qe = (
            (
                len([r for r in classified if r["condition"] == "baseline" and not r.get("execution_success")])
                / len([r for r in classified if r["condition"] == "baseline"])
                * 100
            )
            if any(r["condition"] == "baseline" for r in classified)
            else 0
        )
        strata_qe = (
            (
                len([r for r in classified if r["condition"] == "strata" and not r.get("execution_success")])
                / len([r for r in classified if r["condition"] == "strata"])
                * 100
            )
            if any(r["condition"] == "strata" for r in classified)
            else 0
        )
        delta_qe = strata_qe - baseline_qe

        # SL: schema link correct
        baseline_sl = (
            sum(1 for r in classified if r["condition"] == "baseline" and r.get("schema_link_correct") is True)
            / len([r for r in classified if r["condition"] == "baseline"])
            * 100
            if any(r["condition"] == "baseline" for r in classified)
            else 0
        )
        strata_sl = (
            sum(1 for r in classified if r["condition"] == "strata" and r.get("schema_link_correct") is True)
            / len([r for r in classified if r["condition"] == "strata"])
            * 100
            if any(r["condition"] == "strata" for r in classified)
            else 0
        )
        delta_sl = strata_sl - baseline_sl

        summary_lines.append(f"| EX     | {baseline_ex:5.1f}%  | {strata_ex:5.1f}%  | {delta_ex:+5.1f}pp |")
        summary_lines.append(f"| QE     | {baseline_qe:5.1f}%  | {strata_qe:5.1f}%  | {delta_qe:+5.1f}pp |")
        summary_lines.append(f"| SL     | {baseline_sl:5.1f}%  | {strata_sl:5.1f}%  | {delta_sl:+5.1f}pp |")
    else:
        # Only one condition: show available metrics, mark missing as N/A
        if "baseline" in unique_conditions:
            baseline_ex = (
                sum(1 for r in classified if r["condition"] == "baseline" and is_success(r))
                / len([r for r in classified if r["condition"] == "baseline"])
                * 100
                if any(r["condition"] == "baseline" for r in classified)
                else 0
            )
            summary_lines.append(f"| EX     | {baseline_ex:5.1f}%  | N/A    | N/A    |")
            summary_lines.append(f"| QE     | {0:5.1f}%  | N/A    | N/A    |")
            summary_lines.append(f"| SL     | {0:5.1f}%  | N/A    | N/A    |")
        else:
            strata_ex = (
                sum(1 for r in classified if r["condition"] == "strata" and is_success(r))
                / len([r for r in classified if r["condition"] == "strata"])
                * 100
                if any(r["condition"] == "strata" for r in classified)
                else 0
            )
            summary_lines.append(f"| EX     | N/A    | {strata_ex:5.1f}%  | N/A    |")
            summary_lines.append(f"| QE     | N/A    | {0:5.1f}%  | N/A    |")
            summary_lines.append(f"| SL     | N/A    | {0:5.1f}%  | N/A    |")

    summary_lines.append("")

    # By-tier table
    summary_lines.append("### By tier (EX delta, Strata − Baseline)\n")
    if "baseline" in unique_conditions and "strata" in unique_conditions:
        summary_lines.append("| Tier        | Baseline EX | Strata EX | Delta  |")
        summary_lines.append("|-------------|-------------|-----------|--------|")

        tier_deltas: list[tuple[str, float]] = []
        for tier in unique_tiers:
            baseline_tier_ex = (
                count_by_tier_condition(tier, "baseline", is_success)
                / max(1, len([r for r in classified if r["tier"] == tier and r["condition"] == "baseline"]))
                * 100
            )
            strata_tier_ex = (
                count_by_tier_condition(tier, "strata", is_success)
                / max(1, len([r for r in classified if r["tier"] == tier and r["condition"] == "strata"]))
                * 100
            )
            delta = strata_tier_ex - baseline_tier_ex
            tier_deltas.append((tier, delta))
            summary_lines.append(
                f"| {tier:<11} | {baseline_tier_ex:5.1f}%       | {strata_tier_ex:5.1f}%     | {delta:+5.1f}pp |"
            )

        # Mark top 2 deltas with ← arrow
        if tier_deltas:
            sorted_deltas = sorted(tier_deltas, key=lambda x: x[1], reverse=True)
            top_2_tiers = {sorted_deltas[0][0], sorted_deltas[1][0] if len(sorted_deltas) > 1 else ""}

            # Re-render with arrows
            summary_lines = summary_lines[:-len(unique_tiers)]
            for tier in unique_tiers:
                baseline_tier_ex = (
                    count_by_tier_condition(tier, "baseline", is_success)
                    / max(1, len([r for r in classified if r["tier"] == tier and r["condition"] == "baseline"]))
                    * 100
                )
                strata_tier_ex = (
                    count_by_tier_condition(tier, "strata", is_success)
                    / max(1, len([r for r in classified if r["tier"] == tier and r["condition"] == "strata"]))
                    * 100
                )
                delta = strata_tier_ex - baseline_tier_ex
                arrow = " ← " if tier in top_2_tiers else ""
                summary_lines.append(
                    f"| {tier:<11} | {baseline_tier_ex:5.1f}%       | {strata_tier_ex:5.1f}%     | {delta:+5.1f}pp |{arrow}"
                )
    summary_lines.append("")

    # Failure blocks
    summary_lines.append("---\n")
    summary_lines.append("## Failures\n")
    summary_lines.append("<!-- One block per question where at least one condition failed -->\n")

    for qid in unique_questions:
        # Check if any condition for this question failed
        q_results = [r for r in classified if r["id"] == qid]
        if not any(not is_success(r) for r in q_results):
            continue

        # Build block for this question
        baseline_result = results_by_id_condition.get((qid, "baseline"))
        strata_result = results_by_id_condition.get((qid, "strata"))

        baseline_pass = baseline_result and is_success(baseline_result)
        strata_pass = strata_result and is_success(strata_result)
        baseline_status = "PASS" if baseline_pass else "FAIL" if baseline_result else "NOT RUN"
        strata_status = "PASS" if strata_pass else "FAIL" if strata_result else "NOT RUN"

        # Use first result to get shared info
        first_result = baseline_result or strata_result
        if not first_result:
            continue

        summary_lines.append(f"\n### {first_result['id']} · {first_result['tier']} · BASELINE {baseline_status} / STRATA {strata_status}\n")
        summary_lines.append(f"**Question:** {first_result['natural_language']}\n")

        # Baseline SQL
        baseline_sql = "(no baseline result)" if not baseline_result else baseline_result.get("generated_sql") or "(execution failed)"
        summary_lines.append(f"**Baseline SQL:**")
        summary_lines.append("```sql")
        summary_lines.append(baseline_sql)
        summary_lines.append("```\n")

        # Strata SQL
        strata_sql = "(no strata result)" if not strata_result else strata_result.get("generated_sql") or "(execution failed)"
        summary_lines.append(f"**Strata SQL:**")
        summary_lines.append("```sql")
        summary_lines.append(strata_sql)
        summary_lines.append("```\n")

        # Reference SQL
        summary_lines.append(f"**Reference SQL:**")
        summary_lines.append("```sql")
        summary_lines.append(first_result["reference_sql"])
        summary_lines.append("```\n")

        # Failure modes
        baseline_failure = baseline_result and classify_failure(baseline_result)
        strata_failure = strata_result and classify_failure(strata_result)

        failure_summary = []
        if baseline_failure:
            failure_summary.append(f"Baseline: {baseline_failure}")
        if strata_failure:
            failure_summary.append(f"Strata: {strata_failure}")

        if failure_summary:
            summary_lines.append(f"**Failure modes:** {', '.join(failure_summary)}\n")

        # Notes
        if first_result.get("notes"):
            summary_lines.append(f"**Notes:** {first_result['notes']}\n")

    summary_lines.append("\n---\n")

    # Both-fail summary
    summary_lines.append("## Both-fail summary\n")
    summary_lines.append("Questions where both conditions failed. Most diagnostic for Strata development.\n")
    summary_lines.append("| ID   | Tier     | Failure modes           | Needs manual triage |")
    summary_lines.append("|------|----------|------------------------|---------------------|")

    both_fail_found = False
    for qid in unique_questions:
        baseline_result = results_by_id_condition.get((qid, "baseline"))
        strata_result = results_by_id_condition.get((qid, "strata"))

        if baseline_result and strata_result:
            if not is_success(baseline_result) and not is_success(strata_result):
                both_fail_found = True
                baseline_failure = classify_failure(baseline_result)
                strata_failure = classify_failure(strata_result)
                failures = f"{baseline_failure}, {strata_failure}".replace("None, ", "").replace(", None", "")
                needs_triage = "yes" if strata_failure == "logic or schema error" else "no"
                summary_lines.append(
                    f"| {qid:<4} | {baseline_result['tier']:<8} | {failures:<24} | {needs_triage:<19} |"
                )

    if not both_fail_found:
        summary_lines.append("|      |          |                        |                     |")

    summary_lines.append("")

    # Patterns
    summary_lines.append("\n---\n")
    patterns = generate_patterns(classified)
    summary_lines.append("## Patterns\n")
    summary_lines.append(patterns)
    summary_lines.append("")

    return "\n".join(summary_lines)


def generate_patterns(classified_results: list[dict]) -> str:
    """
    Generate pattern observations based on failure counts.
    Auto-generated factual observations, no speculation.

    Args:
        classified_results: List of results with failure_mode already classified

    Returns:
        Markdown text for the Patterns section
    """
    observations: list[str] = []

    # Count baseline schema hallucinations
    baseline_schema_hal = sum(
        1 for r in classified_results if r.get("condition") == "baseline" and r.get("failure_mode") == "schema hallucination"
    )
    if baseline_schema_hal > 0:
        baseline_total = len([r for r in classified_results if r.get("condition") == "baseline"])
        pct = baseline_schema_hal / baseline_total * 100 if baseline_total > 0 else 0
        observations.append(f"Baseline schema hallucinations: {baseline_schema_hal} of {baseline_total} ({pct:.0f}%)")

    # Count strata schema hallucinations (flag if > 0, unexpected)
    strata_schema_hal = sum(
        1 for r in classified_results if r.get("condition") == "strata" and r.get("failure_mode") == "schema hallucination"
    )
    if strata_schema_hal > 0:
        strata_total = len([r for r in classified_results if r.get("condition") == "strata"])
        pct = strata_schema_hal / strata_total * 100 if strata_total > 0 else 0
        observations.append(
            f"Strata schema hallucinations: {strata_schema_hal} of {strata_total} ({pct:.0f}%) — unexpected, check model completeness"
        )

    # Tier with most failures
    baseline_failures_by_tier: dict[str, int] = {}
    for r in classified_results:
        if r.get("condition") == "baseline" and r.get("failure_mode") is not None:
            tier = r.get("tier")
            baseline_failures_by_tier[tier] = baseline_failures_by_tier.get(tier, 0) + 1

    if baseline_failures_by_tier:
        worst_tier = max(baseline_failures_by_tier, key=baseline_failures_by_tier.get)
        worst_count = baseline_failures_by_tier[worst_tier]
        tier_total = len([r for r in classified_results if r.get("condition") == "baseline" and r.get("tier") == worst_tier])
        observations.append(f"Tier with most baseline failures: {worst_tier} ({worst_count} of {tier_total} failed)")

    strata_failures_by_tier: dict[str, int] = {}
    for r in classified_results:
        if r.get("condition") == "strata" and r.get("failure_mode") is not None:
            tier = r.get("tier")
            strata_failures_by_tier[tier] = strata_failures_by_tier.get(tier, 0) + 1

    if strata_failures_by_tier:
        worst_tier = max(strata_failures_by_tier, key=strata_failures_by_tier.get)
        worst_count = strata_failures_by_tier[worst_tier]
        tier_total = len([r for r in classified_results if r.get("condition") == "strata" and r.get("tier") == worst_tier])
        observations.append(f"Tier with most strata failures: {worst_tier} ({worst_count} of {tier_total} failed)")

    # Both-fail count
    questions_both_fail: set[str] = set()
    seen_questions: dict[str, dict[str, bool]] = {}

    for r in classified_results:
        qid = r.get("id")
        if qid not in seen_questions:
            seen_questions[qid] = {"baseline_fail": False, "strata_fail": False}

        if r.get("condition") == "baseline" and r.get("failure_mode") is not None:
            seen_questions[qid]["baseline_fail"] = True
        if r.get("condition") == "strata" and r.get("failure_mode") is not None:
            seen_questions[qid]["strata_fail"] = True

    for qid, fails in seen_questions.items():
        if fails["baseline_fail"] and fails["strata_fail"]:
            questions_both_fail.add(qid)

    if questions_both_fail:
        observations.append(f"Both-fail questions: {len(questions_both_fail)} — review manually for spec gaps")

    # SL parse failures
    sl_parse_failures = sum(1 for r in classified_results if r.get("schema_link_correct") is None)
    if sl_parse_failures > 0:
        total = len(classified_results)
        pct = sl_parse_failures / total * 100 if total > 0 else 0
        if pct > 20:
            observations.append(
                f"SL parse failures: {sl_parse_failures} of {total} ({pct:.0f}%) — high rate, sqlglot may need dialect tuning"
            )
        else:
            observations.append(f"SL parse failures: {sl_parse_failures} of {total} ({pct:.0f}%) — within acceptable range")

    if not observations:
        observations.append("No significant patterns detected.")

    return "\n".join(observations)


def write_report(results: list[dict], run_id: str, output_dir: str) -> str:
    """
    Classify failures, generate report, write to {output_dir}/report_{run_id}.md.

    Args:
        results: List of per-question result dicts from eval
        run_id: Timestamp identifier for the run
        output_dir: Directory to write report.md to

    Returns:
        Path to the written report file
    """
    output_path = Path(output_dir) / f"report_{run_id}.md"
    report_content = generate_report(results, run_id)

    with output_path.open("w", encoding="utf-8") as handle:
        handle.write(report_content)
        if not report_content.endswith("\n"):
            handle.write("\n")

    return str(output_path)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("usage: reporter.py <results.json> <run_id>", file=sys.stderr)
        sys.exit(1)

    results_file = Path(sys.argv[1])
    run_id = sys.argv[2]

    if not results_file.exists():
        print(f"error: {results_file} not found", file=sys.stderr)
        sys.exit(1)

    with results_file.open("r", encoding="utf-8") as handle:
        all_results = json.load(handle)

    report_content = generate_report(all_results, run_id)
    print(report_content)
