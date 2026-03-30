import sys
import tempfile
import unittest
from pathlib import Path

import yaml

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from smif_loader import load_merged, load_semantic


def _base_semantic() -> dict:
    return {
        "smif_version": "0.1.0",
        "domain": {
            "name": "movies",
            "description": "Movie catalog",
            "provenance": {"source_type": "llm_inferred", "confidence": 0.9},
        },
        "models": [
            {
                "model_id": "movies",
                "name": "movies",
                "label": "Movies",
                "grain": "One row per movie",
                "description": "Movie facts",
                "physical_source": {"schema": "public", "table": "movies"},
                "primary_key": ["id"],
                "ddl_fingerprint": "fp_movies",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "bigint",
                        "role": "identifier",
                        "label": "Movie ID",
                        "description": "Primary key",
                        "provenance": {"source_type": "llm_inferred", "confidence": 0.9},
                    },
                    {
                        "name": "imdb_rating",
                        "data_type": "numeric",
                        "role": "dimension",
                        "label": "IMDb Rating",
                        "description": "Original rating description",
                        "provenance": {"source_type": "llm_inferred", "confidence": 0.9},
                    },
                ],
                "provenance": {"source_type": "llm_inferred", "confidence": 0.9},
            },
            {
                "model_id": "ratings",
                "name": "ratings",
                "label": "Ratings",
                "grain": "One row per movie rating",
                "description": "Ratings by source",
                "physical_source": {"schema": "public", "table": "ratings"},
                "primary_key": ["movie_id"],
                "ddl_fingerprint": "fp_ratings",
                "columns": [
                    {
                        "name": "movie_id",
                        "data_type": "bigint",
                        "role": "identifier",
                        "label": "Movie ID",
                        "description": "Movie reference",
                        "provenance": {"source_type": "llm_inferred", "confidence": 0.9},
                    }
                ],
                "provenance": {"source_type": "llm_inferred", "confidence": 0.9},
            },
        ],
        "relationships": [
            {
                "relationship_id": "movies_to_ratings",
                "from_model": "ratings",
                "from_column": "movie_id",
                "to_model": "movies",
                "to_column": "id",
                "relationship_type": "many_to_one",
                "join_condition": "ratings.movie_id = movies.id",
                "description": "Ratings join",
                "provenance": {"source_type": "llm_inferred", "confidence": 0.9},
            }
        ],
    }


def _write_yaml(path: Path, payload: dict) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


class SmifLoaderTests(unittest.TestCase):
    def test_load_merged_without_corrections_returns_unchanged_model(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            semantic_path = root / "semantic.yaml"
            corrections_path = root / "missing-corrections.yaml"
            semantic = _base_semantic()
            _write_yaml(semantic_path, semantic)

            loaded_semantic = load_semantic(str(semantic_path))
            merged = load_merged(str(semantic_path), str(corrections_path))

            self.assertEqual(loaded_semantic, semantic)
            self.assertEqual(merged, semantic)
            self.assertIsNot(merged, loaded_semantic)

    def test_description_override_on_column_is_applied(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            semantic_path = root / "semantic.yaml"
            corrections_path = root / "corrections.yaml"
            _write_yaml(semantic_path, _base_semantic())
            _write_yaml(
                corrections_path,
                {
                    "smif_version": "0.1.0",
                    "corrections": [
                        {
                            "correction_id": "corr_desc",
                            "target_type": "column",
                            "target_id": "movies.imdb_rating",
                            "correction_type": "description_override",
                            "new_value": "Average IMDb score for the movie.",
                            "source": "user_defined",
                            "status": "approved",
                        }
                    ],
                },
            )

            merged = load_merged(str(semantic_path), str(corrections_path))

            rating_column = merged["models"][0]["columns"][1]
            self.assertEqual(rating_column["description"], "Average IMDb score for the movie.")
            self.assertEqual(rating_column["provenance"]["source_type"], "user_defined")

    def test_role_override_on_column_is_applied(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            semantic_path = root / "semantic.yaml"
            corrections_path = root / "corrections.yaml"
            _write_yaml(semantic_path, _base_semantic())
            _write_yaml(
                corrections_path,
                {
                    "smif_version": "0.1.0",
                    "corrections": [
                        {
                            "correction_id": "corr_role",
                            "target_type": "column",
                            "target_id": "movies.imdb_rating",
                            "correction_type": "role_override",
                            "new_value": "measure",
                            "source": "user_defined",
                            "status": "approved",
                        }
                    ],
                },
            )

            merged = load_merged(str(semantic_path), str(corrections_path))

            self.assertEqual(merged["models"][0]["columns"][1]["role"], "measure")

    def test_suppress_on_model_excludes_it_from_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            semantic_path = root / "semantic.yaml"
            corrections_path = root / "corrections.yaml"
            _write_yaml(semantic_path, _base_semantic())
            _write_yaml(
                corrections_path,
                {
                    "smif_version": "0.1.0",
                    "corrections": [
                        {
                            "correction_id": "corr_suppress_model",
                            "target_type": "model",
                            "target_id": "ratings",
                            "correction_type": "suppress",
                            "new_value": True,
                            "source": "user_defined",
                            "status": "approved",
                        }
                    ],
                },
            )

            merged = load_merged(str(semantic_path), str(corrections_path))

            self.assertEqual([model["model_id"] for model in merged["models"]], ["movies"])
            self.assertEqual(merged["relationships"], [])

    def test_suppress_on_column_excludes_it_from_parent_model(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            semantic_path = root / "semantic.yaml"
            corrections_path = root / "corrections.yaml"
            semantic = _base_semantic()
            semantic["relationships"].append(
                {
                    "relationship_id": "movies_rating_column",
                    "from_model": "movies",
                    "from_column": "imdb_rating",
                    "to_model": "ratings",
                    "to_column": "movie_id",
                    "relationship_type": "one_to_many",
                    "join_condition": "movies.id = ratings.movie_id",
                    "provenance": {"source_type": "llm_inferred", "confidence": 0.9},
                }
            )
            _write_yaml(semantic_path, semantic)
            _write_yaml(
                corrections_path,
                {
                    "smif_version": "0.1.0",
                    "corrections": [
                        {
                            "correction_id": "corr_suppress_column",
                            "target_type": "column",
                            "target_id": "movies.imdb_rating",
                            "correction_type": "suppress",
                            "new_value": True,
                            "source": "user_defined",
                            "status": "approved",
                        }
                    ],
                },
            )

            merged = load_merged(str(semantic_path), str(corrections_path))

            movie_columns = [column["name"] for column in merged["models"][0]["columns"]]
            self.assertEqual(movie_columns, ["id"])
            relationship_ids = [relationship["relationship_id"] for relationship in merged["relationships"]]
            self.assertEqual(relationship_ids, ["movies_to_ratings"])

    def test_pending_corrections_are_not_applied(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            semantic_path = root / "semantic.yaml"
            corrections_path = root / "corrections.yaml"
            _write_yaml(semantic_path, _base_semantic())
            _write_yaml(
                corrections_path,
                {
                    "smif_version": "0.1.0",
                    "corrections": [
                        {
                            "correction_id": "corr_pending",
                            "target_type": "column",
                            "target_id": "movies.imdb_rating",
                            "correction_type": "description_override",
                            "new_value": "Pending description",
                            "source": "user_defined",
                            "status": "pending",
                        }
                    ],
                },
            )

            merged = load_merged(str(semantic_path), str(corrections_path))

            self.assertEqual(merged["models"][0]["columns"][1]["description"], "Original rating description")

    def test_llm_suggested_corrections_are_never_applied(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            semantic_path = root / "semantic.yaml"
            corrections_path = root / "corrections.yaml"
            _write_yaml(semantic_path, _base_semantic())
            _write_yaml(
                corrections_path,
                {
                    "smif_version": "0.1.0",
                    "corrections": [
                        {
                            "correction_id": "corr_llm",
                            "target_type": "column",
                            "target_id": "movies.imdb_rating",
                            "correction_type": "description_override",
                            "new_value": "Suggested description",
                            "source": "llm_suggested",
                            "status": "approved",
                        }
                    ],
                },
            )

            merged = load_merged(str(semantic_path), str(corrections_path))

            self.assertEqual(merged["models"][0]["columns"][1]["description"], "Original rating description")


if __name__ == "__main__":
    unittest.main()