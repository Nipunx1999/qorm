"""Tests for qorm.qns._resolver — parsing, caching, filtering, and failover."""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from qorm.exc import QNSRegistryError
from qorm.qns._registry import RegistryNode
from qorm.qns._resolver import (
    _parse_service_rows,
    _load_cache,
    _save_cache,
    _fetch_from_registry,
    resolve_services,
    filter_by_prefix,
)


class TestParseServiceRows:
    def test_dict_response(self) -> None:
        raw = {
            "__table__": True,
            "dataset": ["EMR", "FXR"],
            "cluster": ["SVC", "SVC"],
            "dbtype": ["HDB", "RDB"],
            "node": ["1", "2"],
            "host": ["h1", "h2"],
            "port": [5010, 5011],
            "ssl": ["tls", "none"],
            "ip": ["1.1.1.1", "2.2.2.2"],
            "env": ["prod", "prod"],
        }
        rows = _parse_service_rows(raw)
        assert len(rows) == 2
        assert rows[0]["dataset"] == "EMR"
        assert rows[1]["port"] == 5011
        assert rows[0]["ssl"] == "tls"

    def test_empty_dict(self) -> None:
        raw = {"__table__": True}
        rows = _parse_service_rows(raw)
        assert rows == []

    def test_unexpected_type_raises(self) -> None:
        with pytest.raises(QNSRegistryError, match="Unexpected"):
            _parse_service_rows("not a dict")


class TestFilterByPrefix:
    ROWS = [
        {"dataset": "EMR", "cluster": "SVC", "dbtype": "HDB", "node": "1"},
        {"dataset": "EMR", "cluster": "SVC", "dbtype": "RDB", "node": "1"},
        {"dataset": "FXR", "cluster": "SVC", "dbtype": "HDB", "node": "1"},
    ]

    def test_no_prefix_returns_all(self) -> None:
        assert len(filter_by_prefix(self.ROWS, ())) == 3

    def test_one_prefix(self) -> None:
        result = filter_by_prefix(self.ROWS, ("EMR",))
        assert len(result) == 2
        assert all(r["dataset"] == "EMR" for r in result)

    def test_two_prefixes(self) -> None:
        result = filter_by_prefix(self.ROWS, ("EMR", "SVC"))
        assert len(result) == 2

    def test_three_prefixes(self) -> None:
        result = filter_by_prefix(self.ROWS, ("EMR", "SVC", "HDB"))
        assert len(result) == 1
        assert result[0]["dbtype"] == "HDB"

    def test_case_insensitive(self) -> None:
        result = filter_by_prefix(self.ROWS, ("emr",))
        assert len(result) == 2

    def test_prefix_matching(self) -> None:
        result = filter_by_prefix(self.ROWS, ("E",))
        assert len(result) == 2  # EMR matches "E"

    def test_no_match(self) -> None:
        result = filter_by_prefix(self.ROWS, ("ZZZ",))
        assert len(result) == 0


class TestFileCache:
    def test_save_and_load(self, tmp_path: Path) -> None:
        rows = [{"dataset": "A", "host": "h1", "port": 5000}]
        with patch("qorm.qns._resolver.CACHE_DIR", tmp_path):
            _save_cache("fx", "prod", rows)
            loaded = _load_cache("fx", "prod", cache_ttl=3600)
        assert loaded == rows

    def test_expired_cache_returns_none(self, tmp_path: Path) -> None:
        cache_file = tmp_path / "fx_prod.json"
        payload = {"timestamp": time.time() - 100, "services": [{"x": 1}]}
        cache_file.write_text(json.dumps(payload), encoding="utf-8")
        with patch("qorm.qns._resolver.CACHE_DIR", tmp_path):
            result = _load_cache("fx", "prod", cache_ttl=50)
        assert result is None

    def test_fresh_cache_returns_data(self, tmp_path: Path) -> None:
        cache_file = tmp_path / "fx_prod.json"
        payload = {"timestamp": time.time(), "services": [{"x": 1}]}
        cache_file.write_text(json.dumps(payload), encoding="utf-8")
        with patch("qorm.qns._resolver.CACHE_DIR", tmp_path):
            result = _load_cache("fx", "prod", cache_ttl=3600)
        assert result == [{"x": 1}]

    def test_corrupt_cache_returns_none(self, tmp_path: Path) -> None:
        cache_file = tmp_path / "fx_prod.json"
        cache_file.write_text("not valid json", encoding="utf-8")
        with patch("qorm.qns._resolver.CACHE_DIR", tmp_path):
            result = _load_cache("fx", "prod", cache_ttl=3600)
        assert result is None

    def test_missing_cache_returns_none(self, tmp_path: Path) -> None:
        with patch("qorm.qns._resolver.CACHE_DIR", tmp_path):
            result = _load_cache("fx", "prod", cache_ttl=3600)
        assert result is None


def _make_node(host: str = "h1", port: int = 5001) -> RegistryNode:
    return RegistryNode(
        dataset="REG", cluster="SVC", dbtype="QNS", node="1",
        host=host, port=port, port_env="P", env="prod",
    )


class TestFetchFromRegistry:
    @patch("qorm.qns._resolver.Session")
    def test_first_node_succeeds(self, mock_session_cls: MagicMock) -> None:
        ctx = MagicMock()
        ctx.raw.return_value = {
            "dataset": ["EMR"],
            "cluster": ["SVC"],
            "dbtype": ["HDB"],
            "node": ["1"],
            "host": ["h1"],
            "port": [5010],
            "ssl": ["tls"],
            "ip": ["1.1.1.1"],
            "env": ["prod"],
        }
        mock_session_cls.return_value.__enter__ = MagicMock(return_value=ctx)
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

        nodes = [_make_node("h1", 5001), _make_node("h2", 5002)]
        result = _fetch_from_registry(nodes, "user", "pass", 10.0)
        assert len(result) == 1
        assert result[0]["dataset"] == "EMR"
        assert mock_session_cls.call_count == 1
        # Verify it queries .qns.registry
        ctx.raw.assert_called_once_with(".qns.registry")

    @patch("qorm.qns._resolver.Session")
    def test_failover_to_second_node(self, mock_session_cls: MagicMock) -> None:
        good_ctx = MagicMock()
        good_ctx.raw.return_value = {
            "dataset": ["X"], "cluster": ["Y"], "dbtype": ["Z"],
            "node": ["1"], "host": ["h"], "port": [9999],
            "ssl": ["none"], "ip": ["0"], "env": ["prod"],
        }

        call_count = {"n": 0}

        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            mock = MagicMock()
            if call_count["n"] == 1:
                mock.__enter__ = MagicMock(side_effect=Exception("connection refused"))
            else:
                mock.__enter__ = MagicMock(return_value=good_ctx)
            mock.__exit__ = MagicMock(return_value=False)
            return mock

        mock_session_cls.side_effect = side_effect

        nodes = [_make_node("h1", 5001), _make_node("h2", 5002)]
        result = _fetch_from_registry(nodes, "u", "p", 5.0)
        assert len(result) == 1
        assert result[0]["dataset"] == "X"

    @patch("qorm.qns._resolver.Session")
    def test_all_nodes_fail(self, mock_session_cls: MagicMock) -> None:
        mock = MagicMock()
        mock.__enter__ = MagicMock(side_effect=Exception("down"))
        mock.__exit__ = MagicMock(return_value=False)
        mock_session_cls.return_value = mock

        nodes = [_make_node("h1", 5001), _make_node("h2", 5002)]
        with pytest.raises(QNSRegistryError, match="All 2 registry"):
            _fetch_from_registry(nodes, "u", "p", 5.0)


class TestResolveServicesWithCache:
    def test_uses_cache_when_fresh(self, tmp_path: Path) -> None:
        cached_rows = [{"dataset": "CACHED", "host": "h", "port": 1}]
        cache_file = tmp_path / "fx_prod.json"
        payload = {"timestamp": time.time(), "services": cached_rows}
        cache_file.write_text(json.dumps(payload), encoding="utf-8")

        with patch("qorm.qns._resolver.CACHE_DIR", tmp_path):
            result = resolve_services(
                [_make_node()], "u", "p", 10.0,
                market="fx", env="prod", cache_ttl=3600,
            )
        assert result == cached_rows

    @patch("qorm.qns._resolver._fetch_from_registry")
    def test_fetches_when_cache_expired(
        self, mock_fetch: MagicMock, tmp_path: Path
    ) -> None:
        cache_file = tmp_path / "fx_prod.json"
        payload = {"timestamp": time.time() - 1000, "services": []}
        cache_file.write_text(json.dumps(payload), encoding="utf-8")

        fresh_rows = [{"dataset": "FRESH", "host": "h", "port": 2}]
        mock_fetch.return_value = fresh_rows

        with patch("qorm.qns._resolver.CACHE_DIR", tmp_path):
            result = resolve_services(
                [_make_node()], "u", "p", 10.0,
                market="fx", env="prod", cache_ttl=500,
            )
        assert result == fresh_rows
        mock_fetch.assert_called_once()
