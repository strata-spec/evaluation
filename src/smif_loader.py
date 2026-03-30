from __future__ import annotations

import copy
import sys
from pathlib import Path

import yaml

__all__ = ["load_semantic", "load_merged"]

_SUPPORTED_CORRECTIONS = {
    "description_override",
    "label_override",
    "role_override",
    "grain_override",
    "suppress",
}


def load_semantic(semantic_path: str) -> dict:
    """Load semantic.yaml and return the parsed dict. Raises if file missing."""
    return _load_yaml_dict(Path(semantic_path), missing_ok=False)


def load_merged(semantic_path: str, corrections_path: str) -> dict:
    """
    Load semantic.yaml and apply corrections.yaml overlay.
    If corrections_path does not exist, returns semantic model unchanged.
    Replicates ApplyOverlay from internal/overlay/merge.go.
    Returns a deep copy — never mutates the loaded files.
    """
    merged, _ = _load_merged_with_stats(Path(semantic_path), Path(corrections_path))
    return merged


def _apply_correction(model: dict, correction: dict) -> bool:
    correction_type = correction.get("correction_type")
    if correction_type not in _SUPPORTED_CORRECTIONS:
        return False

    target_type = correction.get("target_type")
    target_id = correction.get("target_id")
    new_value = str(correction.get("new_value", "")).strip()

    if correction_type == "description_override":
        return _apply_description_override(model, target_type, target_id, new_value)
    if correction_type == "label_override":
        return _apply_label_override(model, target_type, target_id, new_value)
    if correction_type == "role_override":
        column = _find_column(model, target_type, target_id)
        if column is None:
            return False
        column["role"] = new_value
        _set_user_defined_provenance(column)
        return True
    if correction_type == "grain_override":
        model_obj = _find_model(model, target_type, target_id)
        if model_obj is None:
            return False
        model_obj["grain"] = new_value
        x_properties = model_obj.get("x_properties")
        if isinstance(x_properties, dict):
            x_properties["grain"] = new_value
        _set_user_defined_provenance(model_obj)
        return True
    if correction_type == "suppress":
        return _apply_suppress(model, target_type, target_id)

    return False


def _apply_description_override(model: dict, target_type: str, target_id: str, value: str) -> bool:
    if target_type == "domain":
        domain = model.get("domain")
        if not isinstance(domain, dict):
            return False
        domain["description"] = value
        _set_user_defined_provenance(domain)
        return True

    if target_type == "model":
        model_obj = _find_model(model, target_type, target_id)
        if model_obj is None:
            return False
        model_obj["description"] = value
        _set_user_defined_provenance(model_obj)
        return True

    if target_type == "column":
        column = _find_column(model, target_type, target_id)
        if column is None:
            return False
        column["description"] = value
        _set_user_defined_provenance(column)
        return True

    if target_type == "relationship":
        relationship = _find_relationship(model, target_type, target_id)
        if relationship is None:
            return False
        relationship["description"] = value
        _set_user_defined_provenance(relationship)
        return True

    if target_type == "metric":
        metric = _find_metric(model, target_type, target_id)
        if metric is None:
            return False
        metric["description"] = value
        _set_user_defined_provenance(metric)
        return True

    return False


def _apply_label_override(model: dict, target_type: str, target_id: str, value: str) -> bool:
    if target_type == "model":
        model_obj = _find_model(model, target_type, target_id)
        if model_obj is None:
            return False
        model_obj["label"] = value
        _set_user_defined_provenance(model_obj)
        return True

    if target_type == "column":
        column = _find_column(model, target_type, target_id)
        if column is None:
            return False
        column["label"] = value
        _set_user_defined_provenance(column)
        return True

    return False


def _apply_suppress(model: dict, target_type: str, target_id: str) -> bool:
    if target_type == "model":
        model_obj = _find_model(model, target_type, target_id)
        if model_obj is None:
            return False
        model_obj["suppressed"] = True
        _set_user_defined_provenance(model_obj)
        return True

    if target_type == "column":
        resolved = _find_column_with_parent(model, target_type, target_id)
        if resolved is None:
            return False
        model_obj, column = resolved
        column["suppressed"] = True
        _set_user_defined_provenance(column)
        for relationship in _iter_relationships(model):
            if (
                relationship.get("from_model") == model_obj.get("model_id")
                and relationship.get("from_column") == column.get("name")
            ) or (
                relationship.get("to_model") == model_obj.get("model_id")
                and relationship.get("to_column") == column.get("name")
            ):
                relationship["suppressed"] = True
                _set_user_defined_provenance(relationship)
        return True

    if target_type == "relationship":
        relationship = _find_relationship(model, target_type, target_id)
        if relationship is None:
            return False
        relationship["suppressed"] = True
        _set_user_defined_provenance(relationship)
        return True

    if target_type == "metric":
        metric = _find_metric(model, target_type, target_id)
        if metric is None:
            return False
        metric["suppressed"] = True
        _set_user_defined_provenance(metric)
        return True

    return False


def _find_model(model: dict, target_type: str, target_id: str) -> dict | None:
    if target_type != "model":
        return None
    for item in _iter_models(model):
        if item.get("model_id") == target_id:
            return item
    return None


def _find_column(model: dict, target_type: str, target_id: str) -> dict | None:
    resolved = _find_column_with_parent(model, target_type, target_id)
    if resolved is None:
        return None
    _, column = resolved
    return column


def _find_column_with_parent(model: dict, target_type: str, target_id: str) -> tuple[dict, dict] | None:
    if target_type != "column":
        return None
    model_id, separator, column_name = target_id.partition(".")
    model_id = model_id.strip()
    column_name = column_name.strip()
    if separator != "." or not model_id or not column_name:
        return None

    for item in _iter_models(model):
        if item.get("model_id") != model_id:
            continue
        for column in item.get("columns", []) or []:
            if isinstance(column, dict) and column.get("name") == column_name:
                return item, column
    return None


def _find_relationship(model: dict, target_type: str, target_id: str) -> dict | None:
    if target_type != "relationship":
        return None
    for relationship in _iter_relationships(model):
        if relationship.get("relationship_id") == target_id:
            return relationship
    return None


def _find_metric(model: dict, target_type: str, target_id: str) -> dict | None:
    if target_type != "metric":
        return None
    metrics = model.get("metrics", []) or []
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        if metric.get("name") == target_id or metric.get("metric_id") == target_id:
            return metric
    return None


def _iter_models(model: dict) -> list[dict]:
    return [item for item in model.get("models", []) or [] if isinstance(item, dict)]


def _iter_relationships(model: dict) -> list[dict]:
    return [item for item in model.get("relationships", []) or [] if isinstance(item, dict)]


def _load_merged_with_stats(semantic_path: Path, corrections_path: Path) -> tuple[dict, dict]:
    semantic = load_semantic(str(semantic_path))
    merged = copy.deepcopy(semantic)
    stats = {
        "corrections_applied": 0,
        "suppressed_models": 0,
        "suppressed_columns": 0,
    }

    if corrections_path.exists():
        corrections_file = _load_yaml_dict(corrections_path, missing_ok=False)
        corrections = corrections_file.get("corrections") or []
        if not isinstance(corrections, list):
            raise ValueError("corrections.yaml must contain a list under 'corrections'")
        for correction in corrections:
            if not isinstance(correction, dict):
                continue
            if correction.get("status") != "approved" or correction.get("source") == "llm_suggested":
                continue
            if _apply_correction(merged, correction):
                stats["corrections_applied"] += 1

    suppressed_counts = _prune_suppressed(merged)
    stats.update(suppressed_counts)
    return merged, stats


def _load_yaml_dict(path: Path, missing_ok: bool) -> dict:
    if missing_ok and not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"{path.name} must parse to a mapping")
    return data


def _prune_suppressed(model: dict) -> dict:
    kept_models = []
    available_columns: dict[str, set[str]] = {}
    suppressed_models = 0
    suppressed_columns = 0

    for item in _iter_models(model):
        if item.get("suppressed") is True:
            suppressed_models += 1
            for column in item.get("columns", []) or []:
                if isinstance(column, dict) and column.get("suppressed") is True:
                    suppressed_columns += 1
            continue

        kept_columns = []
        for column in item.get("columns", []) or []:
            if not isinstance(column, dict):
                continue
            if column.get("suppressed") is True:
                suppressed_columns += 1
                continue
            kept_columns.append(column)

        item["columns"] = kept_columns
        kept_models.append(item)
        available_columns[item.get("model_id", "")] = {column.get("name") for column in kept_columns if column.get("name")}

    model["models"] = kept_models

    kept_relationships = []
    for relationship in _iter_relationships(model):
        if relationship.get("suppressed") is True:
            continue
        from_model = relationship.get("from_model")
        to_model = relationship.get("to_model")
        from_column = relationship.get("from_column")
        to_column = relationship.get("to_column")
        if from_model not in available_columns or to_model not in available_columns:
            continue
        if from_column not in available_columns[from_model] or to_column not in available_columns[to_model]:
            continue
        kept_relationships.append(relationship)
    if "relationships" in model:
        model["relationships"] = kept_relationships

    if "metrics" in model and isinstance(model.get("metrics"), list):
        model["metrics"] = [
            metric
            for metric in model["metrics"]
            if isinstance(metric, dict) and metric.get("suppressed") is not True
        ]

    return {
        "suppressed_models": suppressed_models,
        "suppressed_columns": suppressed_columns,
    }


def _set_user_defined_provenance(target: dict) -> None:
    provenance = target.get("provenance")
    if not isinstance(provenance, dict):
        provenance = {}
        target["provenance"] = provenance
    provenance["source_type"] = "user_defined"


def _summary_counts(model: dict) -> tuple[int, int, int]:
    models = _iter_models(model)
    model_count = len(models)
    column_count = sum(len(item.get("columns", []) or []) for item in models)
    relationship_count = len(_iter_relationships(model))
    return model_count, column_count, relationship_count


def _pluralize(count: int, singular: str, plural: str | None = None) -> str:
    if count == 1:
        return singular
    return plural or f"{singular}s"


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python smif_loader.py <semantic.yaml> <corrections.yaml>", file=sys.stderr)
        raise SystemExit(1)

    merged_model, stats = _load_merged_with_stats(Path(sys.argv[1]), Path(sys.argv[2]))
    model_count, column_count, relationship_count = _summary_counts(merged_model)

    print(f"Loaded: {model_count} models, {column_count} columns, {relationship_count} relationships")
    print(f"Corrections applied: {stats['corrections_applied']}")
    print(
        "Suppressed: "
        f"{stats['suppressed_models']} {_pluralize(stats['suppressed_models'], 'model')}, "
        f"{stats['suppressed_columns']} {_pluralize(stats['suppressed_columns'], 'column')}"
    )