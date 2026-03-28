"""
Thin API-facing helpers for the canonical latest dataset bundle.
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import quote

from modules.dataset_export import export_dataset_bundle


_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_LATEST_DIR = "dataset_export"
_CSV_PREVIEW_LIMIT = 25
_QA_REPORT_EXCERPT_FIELDS = (
    "generated_at",
    "summary",
    "review_flag_summary",
    "lineage_completeness",
    "followup_targets_summary",
    "followup_targets",
)
_MEDIA_TYPES = {
    ".csv": "text/csv; charset=utf-8",
    ".json": "application/json",
    ".yaml": "application/x-yaml",
    ".yml": "application/x-yaml",
}


def get_latest_bundle_output_dir() -> Path:
    """Return the one canonical output directory used by the API/dashboard."""
    configured = str(os.environ.get("DATASET_EXPORT_LATEST_DIR", "") or "").strip()
    if configured:
        path = Path(configured).expanduser()
        if not path.is_absolute():
            path = _REPO_ROOT / path
        return path.resolve()
    return (_REPO_ROOT / _DEFAULT_LATEST_DIR).resolve()


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError("Latest dataset bundle not found") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON payload at {path}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected object payload at {path}")
    return payload


def _load_latest_manifest_and_report() -> Tuple[Path, Dict[str, Any], Dict[str, Any]]:
    output_dir = get_latest_bundle_output_dir()
    manifest_path = output_dir / "dataset_manifest.json"
    qa_report_path = output_dir / "qa_report.json"
    if not manifest_path.exists() or not qa_report_path.exists():
        raise FileNotFoundError("Latest dataset bundle not found")
    manifest = _read_json(manifest_path)
    qa_report = _read_json(qa_report_path)
    return output_dir, manifest, qa_report


def _manifest_artifacts(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise RuntimeError("Latest dataset manifest is missing artifacts")
    normalized: List[Dict[str, Any]] = []
    for artifact in artifacts:
        if isinstance(artifact, dict):
            normalized.append(dict(artifact))
    return normalized


def _build_qa_report_excerpt(qa_report: Dict[str, Any]) -> Dict[str, Any]:
    return {field: qa_report.get(field) for field in _QA_REPORT_EXCERPT_FIELDS}


def _artifact_download_path(filename: str) -> str:
    return f"/exports/dataset-bundle/latest/artifacts/{quote(str(filename or ''), safe='')}"


def _artifact_preview_path(filename: str) -> str:
    return f"/exports/dataset-bundle/latest/artifacts/{quote(str(filename or ''), safe='')}/preview"


def _resolve_manifest_artifact(
    output_dir: Path,
    manifest: Dict[str, Any],
    artifact_name: str,
) -> Tuple[Dict[str, Any], Path]:
    requested = str(artifact_name or "").strip()
    for artifact in _manifest_artifacts(manifest):
        filename = str(artifact.get("filename") or "").strip()
        if not filename or filename != requested:
            continue
        artifact_path = (output_dir / filename).resolve()
        if artifact_path.parent != output_dir.resolve():
            break
        return dict(artifact), artifact_path
    raise FileNotFoundError("Dataset bundle artifact not found")


def _is_previewable_artifact(artifact: Dict[str, Any], *, exists: bool) -> bool:
    return exists and str(artifact.get("format") or "").strip().lower() == "csv"


def _enrich_artifact(output_dir: Path, artifact: Dict[str, Any]) -> Dict[str, Any] | None:
    filename = str(artifact.get("filename") or "").strip()
    if not filename:
        return None
    path = output_dir / filename
    exists = path.exists() and path.is_file()
    previewable = _is_previewable_artifact(artifact, exists=exists)
    return {
        **artifact,
        "exists": exists,
        "size_bytes": path.stat().st_size if exists else None,
        "download_path": _artifact_download_path(filename),
        "previewable": previewable,
        "preview_path": _artifact_preview_path(filename) if previewable else None,
    }


def _enrich_artifacts(output_dir: Path, manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for artifact in _manifest_artifacts(manifest):
        enriched_artifact = _enrich_artifact(output_dir, artifact)
        if enriched_artifact is not None:
            enriched.append(enriched_artifact)
    return enriched


def generate_latest_dataset_bundle(
    review_db,
    config: Dict[str, Any],
    *,
    config_path: str,
    min_reviews: int = 100,
    include_deleted: bool = False,
) -> Dict[str, Any]:
    """
    Generate the canonical latest dataset bundle and return the API summary payload.

    This is intentionally a thin wrapper around modules.dataset_export.export_dataset_bundle().
    """
    export_dataset_bundle(
        review_db=review_db,
        config=config,
        config_path=config_path,
        output_dir=get_latest_bundle_output_dir(),
        min_reviews=min_reviews,
        include_deleted=include_deleted,
    )
    return load_latest_dataset_bundle_summary()


def load_latest_dataset_bundle_summary() -> Dict[str, Any]:
    """Load the canonical latest bundle summary for the API/dashboard."""
    output_dir, manifest, qa_report = _load_latest_manifest_and_report()
    return {
        "output_dir": str(output_dir),
        "manifest": manifest,
        "qa_report_excerpt": _build_qa_report_excerpt(qa_report),
        "artifacts": _enrich_artifacts(output_dir, manifest),
    }


def read_latest_dataset_bundle_artifact(artifact_name: str) -> Tuple[bytes, str, str]:
    """Read one manifest-listed artifact from the canonical latest bundle."""
    output_dir, manifest, _qa_report = _load_latest_manifest_and_report()
    _artifact, artifact_path = _resolve_manifest_artifact(output_dir, manifest, artifact_name)
    if not artifact_path.exists() or not artifact_path.is_file():
        raise FileNotFoundError("Dataset bundle artifact not found")

    media_type = _MEDIA_TYPES.get(artifact_path.suffix.lower(), "application/octet-stream")
    return artifact_path.read_bytes(), media_type, artifact_path.name


def preview_latest_dataset_bundle_artifact(artifact_name: str) -> Dict[str, Any]:
    """Return a CSV preview payload for one manifest-listed artifact."""
    output_dir, manifest, _qa_report = _load_latest_manifest_and_report()
    artifact, artifact_path = _resolve_manifest_artifact(output_dir, manifest, artifact_name)
    enriched_artifact = _enrich_artifact(output_dir, artifact)
    if enriched_artifact is None:
        raise FileNotFoundError("Dataset bundle artifact not found")
    if not artifact_path.exists() or not artifact_path.is_file():
        raise FileNotFoundError("Dataset bundle artifact not found")
    if not enriched_artifact["previewable"]:
        raise ValueError("Artifact preview is only available for ready CSV artifacts")

    columns: List[str] = list(artifact.get("columns") or [])
    rows: List[Dict[str, str]] = []
    total_row_count = 0
    with open(artifact_path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames:
            columns = list(reader.fieldnames)
        for row in reader:
            normalized_row = {
                str(key or ""): "" if value is None else str(value)
                for key, value in row.items()
                if key is not None
            }
            if total_row_count < _CSV_PREVIEW_LIMIT:
                rows.append(normalized_row)
            total_row_count += 1

    return {
        "artifact": enriched_artifact,
        "preview": {
            "kind": "csv",
            "columns": columns,
            "rows": rows,
            "sample_row_count": len(rows),
            "total_row_count": total_row_count,
            "truncated": total_row_count > len(rows),
        },
    }
