"""Tests for qorm.qns._resolver — query building, parsing, and failover."""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from qorm.exc import QNSRegistryError
from qorm.qns._registry import RegistryNode
from qorm.qns._resolver import _build_svcs_query, _parse_service_rows, resolve_services


class TestBuildSvcsQuery:
    def test_no_prefixes(self) -> None:
        assert _build_svcs_query(()) == ".qns.registry"

    def test_one_prefix(self) -> None:
        assert _build_svcs_query(("EMR",)) == ".qns.svcs`EMR"

    def test_three_prefixes(self) -> None:
        assert _build_svcs_query(("EMR", "SER", "H")) == ".qns.svcs`EMR`SER`H"


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


def _make_node(host: str = "h1", port: int = 5001) -> RegistryNode:
    return RegistryNode(
        dataset="REG", cluster="SVC", dbtype="QNS", node="1",
        host=host, port=port, port_env="P", env="prod",
    )


class TestResolveServices:
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
        result = resolve_services(nodes, ("EMR",), "user", "pass", 10.0)
        assert len(result) == 1
        assert result[0]["dataset"] == "EMR"
        # Should only have been called once (first node succeeded)
        assert mock_session_cls.call_count == 1

    @patch("qorm.qns._resolver.Session")
    def test_failover_to_second_node(self, mock_session_cls: MagicMock) -> None:
        # First call raises, second succeeds
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
        result = resolve_services(nodes, (), "u", "p", 5.0)
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
            resolve_services(nodes, (), "u", "p", 5.0)
