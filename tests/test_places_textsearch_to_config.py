"""Tests for tools/places_textsearch_to_config.py."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import yaml


def _load_tool_module(module_name: str, rel_path: str):
    root = Path(__file__).resolve().parents[1]
    path = root / rel_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_rank_and_select_composite_dedupe_filter():
    mod = _load_tool_module("places_textsearch_to_config_test_mod", "tools/places_textsearch_to_config.py")
    places = [
        mod.PlaceHit("C", "addr", "pid-c", 4.8, 900),
        mod.PlaceHit("B", "addr", "pid-b", 4.9, 300),
        mod.PlaceHit("A", "addr", "pid-a", 4.9, 500),
        mod.PlaceHit("B-dup", "addr", "pid-b", 5.0, 10),
    ]
    selected = mod._rank_and_select_places(
        mod._dedupe_places(places),
        rank_by="composite",
        min_rating=4.9,
        min_ratings_total=250,
        limit=5,
    )
    assert [p.place_id for p in selected] == ["pid-a", "pid-b"]


def test_name_dedupe_keeps_highest_ratings_total():
    mod = _load_tool_module("places_textsearch_to_config_test_mod_name", "tools/places_textsearch_to_config.py")
    places = [
        mod.PlaceHit("Foo Bar", "Addr 1", "pid-a", 4.9, 180),
        mod.PlaceHit("foo   bar", "Addr 2", "pid-b", 4.4, 500),
        mod.PlaceHit("Foo Bar | Downtown", "Addr 3", "pid-c", 4.8, 320),
        mod.PlaceHit("Another Place", "Addr 4", "pid-d", 4.5, 40),
    ]
    deduped = mod._dedupe_places_by_name_highest_ratings_total(places)
    assert [p.place_id for p in deduped] == ["pid-b", "pid-d"]


def test_query_file_parsing_ignores_comments_and_empty(tmp_path):
    mod = _load_tool_module("places_textsearch_to_config_test_mod_qf", "tools/places_textsearch_to_config.py")
    path = tmp_path / "queries.txt"
    path.write_text("\n# comment\nrestaurants in Changhua\n \nbest restaurants in Changhua\n", encoding="utf-8")
    assert mod._read_query_file(str(path)) == [
        "restaurants in Changhua",
        "best restaurants in Changhua",
    ]


def test_main_supports_query_file_and_writes_batch_config(tmp_path, monkeypatch):
    mod = _load_tool_module("places_textsearch_to_config_test_mod_main", "tools/places_textsearch_to_config.py")

    qfile = tmp_path / "queries.txt"
    qfile.write_text("best restaurants in Changhua\n", encoding="utf-8")

    out_cfg = tmp_path / "config.top50.yaml"
    out_json = tmp_path / "places_top50.json"

    query_results = {
        "restaurants in Changhua City": [
            mod.PlaceHit("A", "Addr A", "pid-a", 4.9, 300),
            mod.PlaceHit("B", "Addr B", "pid-b", 4.7, 1000),
        ],
        "best restaurants in Changhua": [
            mod.PlaceHit("C", "Addr C", "pid-c", 5.0, 20),
            mod.PlaceHit("A duplicate", "Addr A2", "pid-a", 4.1, 9),
        ],
    }

    def _fake_fetch(**kwargs):
        return list(query_results.get(kwargs["query"], []))

    monkeypatch.setattr(mod, "_get_api_key", lambda: "test-key")
    monkeypatch.setattr(mod, "_fetch_places_textsearch", _fake_fetch)

    rc = mod.main(
        [
            "--query", "restaurants in Changhua City",
            "--query-file", str(qfile),
            "--limit", "3",
            "--rank-by", "composite",
            "--dedupe-mode", "name_highest_ratings_total",
            "--min-rating", "4.7",
            "--min-ratings-total", "10",
            "--out-config", str(out_cfg),
            "--out-places-json", str(out_json),
        ]
    )
    assert rc == 0

    places_payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert [p["place_id"] for p in places_payload] == ["pid-c", "pid-a", "pid-b"]

    cfg = yaml.safe_load(out_cfg.read_text(encoding="utf-8"))
    assert cfg["use_mongodb"] is False
    assert cfg["google_maps_auth_mode"] == "cookie"
    assert cfg["fail_on_limited_view"] is True
    assert cfg["debug_on_limited_view"] is True
    assert cfg["debug_artifacts_dir"] == "debug_artifacts"
    assert cfg["max_reviews"] == 25
    assert cfg["max_scroll_attempts"] == 10
    assert cfg["scroll_idle_limit"] == 10
    assert len(cfg["businesses"]) == 3


def test_main_name_dedupe_prefers_highest_ratings_total(tmp_path, monkeypatch):
    mod = _load_tool_module("places_textsearch_to_config_test_mod_name_main", "tools/places_textsearch_to_config.py")

    out_cfg = tmp_path / "config.top50.yaml"
    out_json = tmp_path / "places_top50.json"

    query_results = {
        "restaurants in Changhua City": [
            mod.PlaceHit("Foo Place", "Addr A", "pid-a", 4.8, 200),
            mod.PlaceHit("Foo  Place", "Addr B", "pid-b", 4.4, 900),
            mod.PlaceHit("Bar Place", "Addr C", "pid-c", 4.9, 150),
        ],
    }

    def _fake_fetch(**kwargs):
        return list(query_results.get(kwargs["query"], []))

    monkeypatch.setattr(mod, "_get_api_key", lambda: "test-key")
    monkeypatch.setattr(mod, "_fetch_places_textsearch", _fake_fetch)

    rc = mod.main(
        [
            "--query", "restaurants in Changhua City",
            "--limit", "5",
            "--rank-by", "relevance",
            "--dedupe-mode", "name_highest_ratings_total",
            "--out-config", str(out_cfg),
            "--out-places-json", str(out_json),
        ]
    )
    assert rc == 0

    places_payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert [p["place_id"] for p in places_payload] == ["pid-b", "pid-c"]
