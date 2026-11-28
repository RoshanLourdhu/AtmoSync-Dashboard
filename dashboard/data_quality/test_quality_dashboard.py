"""Unit tests for dashboard/data_quality/visualizations/quality_dashboard.py."""
from __future__ import annotations

import json
import math
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dashboard.data_quality.visualizations import quality_dashboard as qd


def test_load_report_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Data quality report not found"):
        qd.load_report(tmp_path / "does_not_exist.json")


def test_load_report_empty_and_malformed_json(tmp_path: Path) -> None:
    empty_file = tmp_path / "empty.json"
    empty_file.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        qd.load_report(empty_file)

    bad_file = tmp_path / "bad.json"
    bad_file.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ValueError, match="Malformed JSON"):
        qd.load_report(bad_file)

    list_file = tmp_path / "list.json"
    list_file.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    with pytest.raises(ValueError, match="JSON object"):
        qd.load_report(list_file)


def test_create_dashboard_missing_plotly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(qd, "PLOTLY_AVAILABLE", False)
    with pytest.raises(RuntimeError, match="Plotly is not installed"):
        qd.create_dashboard({"total_records": 10})


def test_create_dashboard_rejects_invalid_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(qd, "PLOTLY_AVAILABLE", True)
    monkeypatch.setattr(qd, "go", MagicMock())
    monkeypatch.setattr(qd, "make_subplots", MagicMock())

    with pytest.raises(ValueError, match="must be a dictionary"):
        qd.create_dashboard(["not", "a", "dict"])  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="must be numeric"):
        qd.create_dashboard({"quality_score": "invalid"})

    with pytest.raises(ValueError, match="must be finite"):
        qd.create_dashboard({"quality_score": math.nan})

    with pytest.raises(ValueError, match="outliers"):
        qd.create_dashboard({"total_records": 5, "outliers": ["bad"]})


def test_create_dashboard_handles_empty_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_fig = MagicMock()
    monkeypatch.setattr(qd, "PLOTLY_AVAILABLE", True)
    monkeypatch.setattr(qd, "go", MagicMock())
    monkeypatch.setattr(qd, "make_subplots", MagicMock(return_value=mock_fig))

    fig = qd.create_dashboard({"total_records": 0, "quality_score": 0.0, "outliers": None})
    assert fig is mock_fig
    _, kwargs = mock_fig.update_layout.call_args
    assert "annotations" in kwargs
    assert "Empty dataset" in kwargs["annotations"][0]["text"]


def test_main_returns_nonzero_on_missing_report(tmp_path: Path) -> None:
    with patch("builtins.print"):
        rc = qd.main(tmp_path / "missing.json")
    assert rc == 1
