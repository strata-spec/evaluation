from __future__ import annotations

import sys
import unittest
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from eval import fetch_raw_schema, format_smif_context, _strip_sql_fences


class FormatSmifContextTests(unittest.TestCase):
    def test_includes_relevant_model_when_question_mentions_table_name(self) -> None:
        model = {
            "domain": {"name": "OMDB", "description": "Movie database"},
            "models": [
                {
                    "model_id": "movies",
                    "name": "movies",
                    "label": "Movies",
                    "description": "Primary movie table.",
                    "grain": "One row per movie.",
                    "physical_source": {"table": "movies"},
                    "columns": [{"name": "id", "role": "identifier", "description": "Movie id."}],
                },
                {
                    "model_id": "ratings",
                    "name": "ratings",
                    "label": "Ratings",
                    "description": "Ratings table.",
                    "grain": "One row per rating.",
                    "physical_source": {"table": "ratings"},
                    "columns": [{"name": "movie_id", "role": "identifier", "description": "Movie id."}],
                },
                {
                    "model_id": "people",
                    "name": "people",
                    "label": "People",
                    "description": "People table.",
                    "grain": "One row per person.",
                    "physical_source": {"table": "people"},
                    "columns": [{"name": "id", "role": "identifier", "description": "Person id."}],
                },
                {
                    "model_id": "casts",
                    "name": "casts",
                    "label": "Casts",
                    "description": "Cast table.",
                    "grain": "One row per cast record.",
                    "physical_source": {"table": "casts"},
                    "columns": [{"name": "movie_id", "role": "identifier", "description": "Movie id."}],
                },
            ],
        }

        context = format_smif_context(model, "How many movies are in the database?")

        self.assertIn("Model: movies", context)

    def test_includes_all_models_when_fewer_than_four_exist(self) -> None:
        model = {
            "domain": {"name": "OMDB", "description": "Movie database"},
            "models": [
                {"model_id": "movies", "name": "movies", "columns": []},
                {"model_id": "ratings", "name": "ratings", "columns": []},
                {"model_id": "people", "name": "people", "columns": []},
            ],
        }

        context = format_smif_context(model, "Unrelated question text")

        self.assertIn("Model: movies", context)
        self.assertIn("Model: ratings", context)
        self.assertIn("Model: people", context)

    def test_excludes_irrelevant_models_when_question_is_specific(self) -> None:
        model = {
            "domain": {"name": "OMDB", "description": "Movie database"},
            "models": [
                {
                    "model_id": "movies",
                    "name": "movies",
                    "columns": [{"name": "title", "role": "dimension", "description": "Title."}],
                },
                {
                    "model_id": "ratings",
                    "name": "ratings",
                    "columns": [{"name": "score", "role": "measure", "description": "Score."}],
                },
                {
                    "model_id": "people",
                    "name": "people",
                    "columns": [{"name": "name", "role": "dimension", "description": "Name."}],
                },
                {
                    "model_id": "trailers",
                    "name": "trailers",
                    "columns": [{"name": "url", "role": "dimension", "description": "URL."}],
                },
            ],
        }

        context = format_smif_context(model, "List movie titles")

        self.assertIn("Model: movies", context)
        self.assertNotIn("Model: ratings", context)
        self.assertNotIn("Model: people", context)
        self.assertNotIn("Model: trailers", context)

    def test_includes_relationships_between_included_models(self) -> None:
        model = {
            "domain": {"name": "OMDB", "description": "Movie database"},
            "models": [
                {
                    "model_id": "movies",
                    "name": "movies",
                    "columns": [{"name": "id", "role": "identifier", "description": "Movie id."}],
                },
                {
                    "model_id": "ratings",
                    "name": "ratings",
                    "columns": [{"name": "movie_id", "role": "identifier", "description": "Movie id."}],
                },
                {
                    "model_id": "reviews",
                    "name": "reviews",
                    "columns": [{"name": "review_id", "role": "identifier", "description": "Review id."}],
                },
                {
                    "model_id": "trailers",
                    "name": "trailers",
                    "columns": [{"name": "url", "role": "dimension", "description": "Trailer URL."}],
                },
            ],
            "relationships": [
                {
                    "from_model": "movies",
                    "from_column": "id",
                    "to_model": "ratings",
                    "to_column": "movie_id",
                    "relationship_type": "many-to-one",
                },
                {
                    "from_model": "reviews",
                    "from_column": "review_id",
                    "to_model": "movies",
                    "to_column": "id",
                    "relationship_type": "many-to-one",
                },
            ],
        }

        context = format_smif_context(model, "Show ratings for movies")

        # Pass-1 seeds "movies".
        # Pass-2 expands bidirectionally:
        # - movies→ratings: from="movies" in seed → add ratings ✓
        # - reviews→movies: to="movies" in seed → add reviews ✓
        # Both relationships should be included since both endpoints are now included.
        self.assertIn("movies.id → ratings.movie_id (many-to-one)", context)
        self.assertIn("reviews.review_id → movies.id (many-to-one)", context)
        # Trailers has no relationship to movies, so it should not appear.
        self.assertNotIn("Model: trailers", context)

    def test_relationship_expansion_includes_join_target(self) -> None:
        """Pass-2: question names only one side of a relationship; join target must be included."""
        model = {
            "domain": {"name": "OMDB", "description": "Movie database"},
            "models": [
                {
                    "model_id": "casts",
                    "name": "casts",
                    "label": "Casts",
                    "description": "Cast members.",
                    "columns": [{"name": "movie_id", "role": "identifier", "description": "FK to movies."}],
                },
                {
                    "model_id": "jobs",
                    "name": "jobs",
                    "label": "Jobs",
                    "description": "Job titles held by cast members.",
                    "columns": [{"name": "title", "role": "dimension", "description": "Job title."}],
                },
                {
                    "model_id": "movies",
                    "name": "movies",
                    "label": "Movies",
                    "description": "Movies table.",
                    "columns": [{"name": "id", "role": "identifier", "description": "Movie id."}],
                },
                {
                    "model_id": "ratings",
                    "name": "ratings",
                    "label": "Ratings",
                    "description": "Ratings table.",
                    "columns": [{"name": "score", "role": "measure", "description": "Score."}],
                },
            ],
            "relationships": [
                {
                    "from_model": "casts",
                    "from_column": "job_id",
                    "to_model": "jobs",
                    "to_column": "id",
                    "relationship_type": "many-to-one",
                },
            ],
        }

        # "casts" matches; "jobs" does not appear in the question.
        # After pass-2 expansion via the relationship, "jobs" should be included.
        context = format_smif_context(model, "Find all entries in the casts table and what they did")

        self.assertIn("Model: casts", context)
        self.assertIn("Model: jobs", context)

    def test_relationship_expansion_does_not_recurse(self) -> None:
        """Pass-2: expansion is one hop only. A→B is expanded; B→C is not."""
        model = {
            "domain": {"name": "OMDB", "description": "Movie database"},
            "models": [
                {
                    "model_id": "movies",
                    "name": "movies",
                    "label": "Movies",
                    "description": "Movies table.",
                    "columns": [{"name": "id", "role": "identifier", "description": "Movie id."}],
                },
                {
                    "model_id": "casts",
                    "name": "casts",
                    "label": "Casts",
                    "description": "Cast table.",
                    "columns": [{"name": "movie_id", "role": "identifier", "description": "FK."}],
                },
                {
                    "model_id": "jobs",
                    "name": "jobs",
                    "label": "Jobs",
                    "description": "Job title table.",
                    "columns": [{"name": "title", "role": "dimension", "description": "Title."}],
                },
                {
                    "model_id": "departments",
                    "name": "departments",
                    "label": "Departments",
                    "description": "Department table.",
                    "columns": [{"name": "name", "role": "dimension", "description": "Dept name."}],
                },
            ],
            "relationships": [
                {
                    "from_model": "movies",
                    "from_column": "id",
                    "to_model": "casts",
                    "to_column": "movie_id",
                    "relationship_type": "one-to-many",
                },
                {
                    "from_model": "casts",
                    "from_column": "job_id",
                    "to_model": "jobs",
                    "to_column": "id",
                    "relationship_type": "many-to-one",
                },
                {
                    "from_model": "jobs",
                    "from_column": "dept_id",
                    "to_model": "departments",
                    "to_column": "id",
                    "relationship_type": "many-to-one",
                },
            ],
        }

        # Pass 1 seeds "movies". Pass 2 expands one hop → adds "casts".
        # "jobs" is two hops away; "departments" is three hops. Neither should appear.
        context = format_smif_context(model, "How many movies are there?")

        self.assertIn("Model: movies", context)
        self.assertIn("Model: casts", context)
        self.assertNotIn("Model: jobs", context)
        self.assertNotIn("Model: departments", context)


class StripSqlFencesTests(unittest.TestCase):
    def test_sql_extraction_strips_markdown_code_fences(self) -> None:
        sql = _strip_sql_fences("```sql\nSELECT * FROM movies;\n```")

        self.assertEqual(sql, "SELECT * FROM movies;")

    def test_sql_extraction_returns_raw_text_without_fences(self) -> None:
        sql = _strip_sql_fences("SELECT COUNT(*) FROM movies")

        self.assertEqual(sql, "SELECT COUNT(*) FROM movies")


class FetchRawSchemaTests(unittest.TestCase):
    def test_fetch_raw_schema_returns_empty_string_on_connection_failure(self) -> None:
        schema = fetch_raw_schema("postgresql://invalid:invalid@127.0.0.1:1/does_not_exist")

        self.assertEqual(schema, "")


if __name__ == "__main__":
    unittest.main()
