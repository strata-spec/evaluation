"""
Unit tests for reporter.py
"""

import sys
from pathlib import Path

import json
import tempfile
import unittest

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from reporter import classify_failure, generate_report, generate_patterns, write_report


class ClassifyFailureTests(unittest.TestCase):
    """Test classify_failure function"""

    def test_passing_result_returns_none(self):
        """classify_failure returns None for a passing result"""
        result = {
            "id": "q001",
            "tier": "simple",
            "condition": "baseline",
            "execution_success": True,
            "result_match": True,
            "schema_link_correct": True,
            "failure_mode": None,
        }
        self.assertIsNone(classify_failure(result))

    def test_schema_hallucination_on_execution_failure(self):
        """classify_failure returns 'schema hallucination' when execution_success is False"""
        result = {
            "id": "q002",
            "tier": "ambiguous",
            "condition": "baseline",
            "execution_success": False,
            "result_match": False,
            "schema_link_correct": None,
            "failure_mode": None,
        }
        self.assertEqual(classify_failure(result), "schema hallucination")

    def test_wrong_table_or_column_on_schema_link_false(self):
        """classify_failure returns 'wrong table or column' when schema_link_correct is False"""
        result = {
            "id": "q003",
            "tier": "multi_table",
            "condition": "baseline",
            "execution_success": True,
            "result_match": False,
            "schema_link_correct": False,
            "failure_mode": None,
        }
        self.assertEqual(classify_failure(result), "wrong table or column")

    def test_logic_error_on_correct_schema_wrong_result(self):
        """classify_failure returns 'logic error' when schema_link_correct is True and result mismatch"""
        result = {
            "id": "q004",
            "tier": "metric",
            "condition": "strata",
            "execution_success": True,
            "result_match": False,
            "schema_link_correct": True,
            "failure_mode": None,
        }
        self.assertEqual(classify_failure(result), "logic error")

    def test_logic_or_schema_error_on_none_schema_link(self):
        """classify_failure returns 'logic or schema error' when schema_link_correct is None and mismatch"""
        result = {
            "id": "q005",
            "tier": "gotcha",
            "condition": "baseline",
            "execution_success": True,
            "result_match": False,
            "schema_link_correct": None,
            "failure_mode": None,
        }
        self.assertEqual(classify_failure(result), "logic or schema error")


class GenerateReportTests(unittest.TestCase):
    """Test generate_report function"""

    def test_report_contains_summary_table(self):
        """generate_report contains the summary table with EX, QE, SL metrics"""
        results = [
            {
                "id": "q001",
                "tier": "simple",
                "condition": "baseline",
                "natural_language": "How many movies?",
                "generated_sql": "SELECT COUNT(*) FROM movies",
                "reference_sql": "SELECT COUNT(*) FROM movies",
                "execution_success": True,
                "result_match": True,
                "schema_link_correct": True,
                "failure_mode": None,
                "notes": "test",
            },
            {
                "id": "q001",
                "tier": "simple",
                "condition": "strata",
                "natural_language": "How many movies?",
                "generated_sql": "SELECT COUNT(*) FROM movies",
                "reference_sql": "SELECT COUNT(*) FROM movies",
                "execution_success": True,
                "result_match": True,
                "schema_link_correct": True,
                "failure_mode": None,
                "notes": "test",
            },
        ]
        report = generate_report(results, "2026-03-24T12-00-00")
        self.assertIn("# Strata eval — 2026-03-24T12-00-00", report)
        self.assertIn("## Summary", report)
        self.assertIn("| Metric | Baseline | Strata | Delta |", report)
        self.assertIn("| EX", report)
        self.assertIn("| QE", report)
        self.assertIn("| SL", report)

    def test_report_contains_failure_block_for_failing_question(self):
        """generate_report contains a failure block for a failing question"""
        results = [
            {
                "id": "q001",
                "tier": "simple",
                "condition": "baseline",
                "natural_language": "How many movies?",
                "generated_sql": "SELECT COUNT(*) FROM wrong_table",
                "reference_sql": "SELECT COUNT(*) FROM movies",
                "execution_success": False,
                "result_match": False,
                "schema_link_correct": None,
                "failure_mode": None,
                "notes": "test",
            },
        ]
        report = generate_report(results, "2026-03-24T12-00-00")
        self.assertIn("## Failures", report)
        self.assertIn("### q001 · simple", report)
        self.assertIn("**Question:** How many movies?", report)
        self.assertIn("**Baseline SQL:**", report)
        self.assertIn("```sql", report)

    def test_report_shows_both_fail_table_when_both_conditions_failed(self):
        """generate_report shows both-fail table when both conditions failed on same question"""
        results = [
            {
                "id": "q001",
                "tier": "simple",
                "condition": "baseline",
                "natural_language": "How many movies?",
                "generated_sql": "SELECT COUNT(*) FROM wrong_table",
                "reference_sql": "SELECT COUNT(*) FROM movies",
                "execution_success": False,
                "result_match": False,
                "schema_link_correct": None,
                "failure_mode": None,
                "notes": "test",
            },
            {
                "id": "q001",
                "tier": "simple",
                "condition": "strata",
                "natural_language": "How many movies?",
                "generated_sql": "SELECT COUNT(*) FROM wrong_table",
                "reference_sql": "SELECT COUNT(*) FROM movies",
                "execution_success": False,
                "result_match": False,
                "schema_link_correct": None,
                "failure_mode": None,
                "notes": "test",
            },
        ]
        report = generate_report(results, "2026-03-24T12-00-00")
        self.assertIn("## Both-fail summary", report)
        self.assertIn("| q001 |", report)

    def test_report_with_baseline_only_shows_na_for_strata(self):
        """generate_report shows N/A gracefully when only baseline condition was run"""
        results = [
            {
                "id": "q001",
                "tier": "simple",
                "condition": "baseline",
                "natural_language": "How many movies?",
                "generated_sql": "SELECT COUNT(*) FROM movies",
                "reference_sql": "SELECT COUNT(*) FROM movies",
                "execution_success": True,
                "result_match": True,
                "schema_link_correct": True,
                "failure_mode": None,
                "notes": "test",
            },
        ]
        report = generate_report(results, "2026-03-24T12-00-00")
        self.assertIn("| EX", report)
        self.assertIn("N/A", report)

    def test_report_with_strata_only_shows_na_for_baseline(self):
        """generate_report shows N/A gracefully when only strata condition was run"""
        results = [
            {
                "id": "q001",
                "tier": "simple",
                "condition": "strata",
                "natural_language": "How many movies?",
                "generated_sql": "SELECT COUNT(*) FROM movies",
                "reference_sql": "SELECT COUNT(*) FROM movies",
                "execution_success": True,
                "result_match": True,
                "schema_link_correct": True,
                "failure_mode": None,
                "notes": "test",
            },
        ]
        report = generate_report(results, "2026-03-24T12-00-00")
        self.assertIn("| EX", report)
        self.assertIn("N/A", report)


class GeneratePatternsTests(unittest.TestCase):
    """Test generate_patterns function"""

    def test_patterns_include_baseline_schema_hallucinations(self):
        """generate_patterns includes count of baseline schema hallucinations"""
        results = [
            {
                "id": "q001",
                "tier": "simple",
                "condition": "baseline",
                "execution_success": False,
                "result_match": False,
                "schema_link_correct": None,
                "failure_mode": "schema hallucination",
            },
            {
                "id": "q002",
                "tier": "simple",
                "condition": "baseline",
                "execution_success": True,
                "result_match": True,
                "schema_link_correct": True,
                "failure_mode": None,
            },
        ]
        patterns = generate_patterns(results)
        self.assertIn("Baseline schema hallucinations", patterns)
        self.assertIn("1 of 2", patterns)

    def test_patterns_include_strata_schema_hallucinations_when_found(self):
        """generate_patterns flags strata schema hallucinations as unexpected"""
        results = [
            {
                "id": "q001",
                "tier": "simple",
                "condition": "strata",
                "execution_success": False,
                "result_match": False,
                "schema_link_correct": None,
                "failure_mode": "schema hallucination",
            },
        ]
        patterns = generate_patterns(results)
        self.assertIn("Strata schema hallucinations", patterns)
        self.assertIn("unexpected", patterns)

    def test_patterns_include_both_fail_count(self):
        """generate_patterns includes both-fail question count"""
        results = [
            {
                "id": "q001",
                "tier": "simple",
                "condition": "baseline",
                "execution_success": False,
                "result_match": False,
                "schema_link_correct": None,
                "failure_mode": "schema hallucination",
            },
            {
                "id": "q001",
                "tier": "simple",
                "condition": "strata",
                "execution_success": False,
                "result_match": False,
                "schema_link_correct": None,
                "failure_mode": "schema hallucination",
            },
        ]
        patterns = generate_patterns(results)
        self.assertIn("Both-fail questions", patterns)

    def test_patterns_include_sl_parse_failures(self):
        """generate_patterns includes SL parse failure rate"""
        results = [
            {
                "id": "q001",
                "tier": "simple",
                "condition": "baseline",
                "execution_success": True,
                "result_match": False,
                "schema_link_correct": None,
                "failure_mode": "logic or schema error",
            },
            {
                "id": "q002",
                "tier": "simple",
                "condition": "baseline",
                "execution_success": True,
                "result_match": True,
                "schema_link_correct": True,
                "failure_mode": None,
            },
        ]
        patterns = generate_patterns(results)
        self.assertIn("SL parse failures", patterns)


class WriteReportTests(unittest.TestCase):
    """Test write_report function"""

    def test_write_report_creates_file(self):
        """write_report creates a report file in the output directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            results = [
                {
                    "id": "q001",
                    "tier": "simple",
                    "condition": "baseline",
                    "natural_language": "How many movies?",
                    "generated_sql": "SELECT COUNT(*) FROM movies",
                    "reference_sql": "SELECT COUNT(*) FROM movies",
                    "execution_success": True,
                    "result_match": True,
                    "schema_link_correct": True,
                    "failure_mode": None,
                    "notes": "test",
                },
            ]
            report_path = write_report(results, "2026-03-24T12-00-00", tmpdir)
            self.assertTrue(Path(report_path).exists())
            self.assertIn("report_2026-03-24T12-00-00.md", report_path)

    def test_write_report_returns_path(self):
        """write_report returns the path to the written file"""
        with tempfile.TemporaryDirectory() as tmpdir:
            results = [
                {
                    "id": "q001",
                    "tier": "simple",
                    "condition": "baseline",
                    "natural_language": "How many movies?",
                    "generated_sql": "SELECT COUNT(*) FROM movies",
                    "reference_sql": "SELECT COUNT(*) FROM movies",
                    "execution_success": True,
                    "result_match": True,
                    "schema_link_correct": True,
                    "failure_mode": None,
                    "notes": "test",
                }
            ]
            report_path = write_report(results, "2026-03-24T12-00-00", tmpdir)
            self.assertTrue(isinstance(report_path, str))
            self.assertTrue(report_path.endswith(".md"))


if __name__ == "__main__":
    unittest.main()
