from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import yaml

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from scorer import (
    compute_metrics,
    compute_metrics_by_tier,
    load_questions,
    normalise_result,
    results_match,
    schema_link_score,
)


def _write_questions(path: Path, questions: list[dict]) -> None:
    payload = {"questions": questions}
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _sample_questions() -> list[dict]:
    return [
        {
            "id": "q001",
            "tier": "simple",
            "natural_language": "How many movies are in the database?",
            "reference_sql": "SELECT COUNT(*) FROM movies",
        },
        {
            "id": "q002",
            "tier": "ambiguous",
            "natural_language": "What is the rating?",
            "reference_sql": "SELECT imdb_rating FROM movies",
        },
        {
            "id": "q003",
            "tier": "simple",
            "natural_language": "Skipped question",
            "reference_sql": "SELECT 1",
            "skip": True,
        },
    ]


class LoadQuestionsTests(unittest.TestCase):
    def test_load_questions_returns_all_questions_without_tier_filter(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "questions.yaml"
            _write_questions(path, _sample_questions())

            questions = load_questions(str(path))

            self.assertEqual([question["id"] for question in questions], ["q001", "q002"])

    def test_load_questions_filters_by_tier(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "questions.yaml"
            _write_questions(path, _sample_questions())

            questions = load_questions(str(path), tier="ambiguous")

            self.assertEqual([question["id"] for question in questions], ["q002"])

    def test_load_questions_raises_on_missing_required_field(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "questions.yaml"
            _write_questions(
                path,
                [
                    {
                        "id": "q001",
                        "tier": "simple",
                        "natural_language": "Broken question",
                    }
                ],
            )

            with self.assertRaises(ValueError):
                load_questions(str(path))

    def test_load_questions_excludes_skip_true(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "questions.yaml"
            _write_questions(path, _sample_questions())

            questions = load_questions(str(path))

            self.assertNotIn("q003", [question["id"] for question in questions])


class NormaliseResultTests(unittest.TestCase):
    def test_normalise_result_lowercases_strings_and_column_names(self) -> None:
        normalised = normalise_result(["Movie", "Title"], [("  JAWS ", "  BlockBuster ")])

        self.assertEqual(normalised, [{"movie": "jaws", "title": "blockbuster"}])

    def test_normalise_result_rounds_floats_to_two_dp(self) -> None:
        normalised = normalise_result(["rating"], [(8.456,)])

        self.assertEqual(normalised, [{"rating": 8.46}])

    def test_normalise_result_sorts_rows_deterministically(self) -> None:
        normalised = normalise_result(["name"], [("Zulu",), ("alpha",), ("Bravo",)])

        self.assertEqual(
            normalised,
            [{"name": "alpha"}, {"name": "bravo"}, {"name": "zulu"}],
        )


class ResultsMatchTests(unittest.TestCase):
    def test_results_match_returns_true_for_identical_sets(self) -> None:
        self.assertTrue(
            results_match(
                ["count"],
                [(5,)],
                ["count"],
                [(5,)],
            )
        )

    def test_results_match_returns_true_for_different_column_order(self) -> None:
        self.assertTrue(
            results_match(
                ["id", "title"],
                [(1, "Jaws")],
                ["title", "id"],
                [("jaws", 1)],
            )
        )

    def test_results_match_returns_false_for_different_row_counts(self) -> None:
        self.assertFalse(
            results_match(
                ["id"],
                [(1,), (2,)],
                ["id"],
                [(1,)],
            )
        )

    def test_results_match_returns_false_for_different_values(self) -> None:
        self.assertFalse(
            results_match(
                ["id", "title"],
                [(1, "Jaws")],
                ["id", "title"],
                [(1, "Alien")],
            )
        )


class SchemaLinkScoreTests(unittest.TestCase):
    def test_schema_link_score_returns_true_when_tables_match(self) -> None:
        score = schema_link_score(
            "SELECT COUNT(*) FROM movies",
            "SELECT id FROM movies",
        )

        self.assertTrue(score)

    def test_schema_link_score_returns_false_for_wrong_table(self) -> None:
        score = schema_link_score(
            "SELECT COUNT(*) FROM movies",
            "SELECT COUNT(*) FROM ratings",
        )

        self.assertFalse(score)

    def test_schema_link_score_returns_none_on_unparseable_sql(self) -> None:
        score = schema_link_score(
            "SELECT COUNT(*) FROM movies",
            "SELECT FROM WHERE",
        )

        self.assertIsNone(score)


class MetricsTests(unittest.TestCase):
    def test_compute_metrics_returns_expected_values(self) -> None:
        results = [
            {"execution_success": True, "result_match": True, "schema_link_correct": True},
            {"execution_success": True, "result_match": False, "schema_link_correct": False},
            {"execution_success": False, "result_match": False, "schema_link_correct": None},
            {"execution_success": True, "result_match": True, "schema_link_correct": True},
        ]

        metrics = compute_metrics(results)

        self.assertEqual(metrics, {"EX": 0.5, "QE": 0.25, "SL": 2 / 3})

    def test_compute_metrics_returns_zeros_for_empty_input(self) -> None:
        self.assertEqual(compute_metrics([]), {"EX": 0.0, "QE": 0.0, "SL": 0.0})

    def test_compute_metrics_by_tier_groups_correctly(self) -> None:
        results = [
            {
                "tier": "simple",
                "execution_success": True,
                "result_match": True,
                "schema_link_correct": True,
            },
            {
                "tier": "simple",
                "execution_success": False,
                "result_match": False,
                "schema_link_correct": None,
            },
            {
                "tier": "metric",
                "execution_success": True,
                "result_match": False,
                "schema_link_correct": False,
            },
        ]

        by_tier = compute_metrics_by_tier(results)

        self.assertEqual(
            by_tier,
            {
                "metric": {"EX": 0.0, "QE": 0.0, "SL": 0.0, "count": 1},
                "simple": {"EX": 0.5, "QE": 0.5, "SL": 1.0, "count": 2},
            },
        )


if __name__ == "__main__":
    unittest.main()