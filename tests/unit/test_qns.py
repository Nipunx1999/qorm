"""Tests for qorm.qns — QNS class and ServiceInfo."""

from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from qorm.engine import Engine
from qorm.exc import QNSConfigError, QNSServiceNotFoundError
from qorm.qns import QNS, ServiceInfo


class TestServiceInfo:
    def test_tls_true(self) -> None:
        svc = ServiceInfo(
            dataset="EMR", cluster="SVC", dbtype="HDB", node="1",
            host="h1", port=5010, ssl="TLS", ip="1.1.1.1", env="prod",
        )
        assert svc.tls is True

    def test_tls_false(self) -> None:
        svc = ServiceInfo(
            dataset="EMR", cluster="SVC", dbtype="HDB", node="1",
            host="h1", port=5010, ssl="none", ip="1.1.1.1", env="prod",
        )
        assert svc.tls is False

    def test_fqn(self) -> None:
        svc = ServiceInfo(
            dataset="EMRATESCV", cluster="SERVICE", dbtype="HDB", node="1",
            host="h1", port=5010, ssl="tls", ip="1.1.1.1", env="prod",
        )
        assert svc.fqn == "EMRATESCV.SERVICE.HDB.1"


def _make_service_rows(*names: str) -> list[dict]:
    rows = []
    for i, name in enumerate(names):
        parts = name.split(".")
        rows.append({
            "dataset": parts[0],
            "cluster": parts[1],
            "dbtype": parts[2],
            "node": parts[3],
            "host": f"host{i}",
            "port": 5010 + i,
            "ssl": "tls",
            "ip": f"10.0.0.{i}",
            "env": "prod",
        })
    return rows


class TestQNSInit:
    @patch("qorm.qns.load_registry_nodes")
    def test_loads_registry_nodes(self, mock_load: MagicMock, tmp_path) -> None:
        mock_load.return_value = [MagicMock()]
        qns = QNS(market="fx", env="prod", data_dir=tmp_path)
        mock_load.assert_called_once_with("fx", "prod", data_dir=tmp_path)


class TestQNSLookup:
    @patch("qorm.qns.resolve_services")
    @patch("qorm.qns.load_registry_nodes")
    def test_returns_service_info_list(
        self, mock_load: MagicMock, mock_resolve: MagicMock
    ) -> None:
        mock_load.return_value = [MagicMock()]
        mock_resolve.return_value = _make_service_rows(
            "EMR.SVC.HDB.1", "EMR.SVC.HDB.2"
        )
        qns = QNS(market="fx", env="prod")
        services = qns.lookup("EMR", "SVC", "HDB")
        assert len(services) == 2
        assert all(isinstance(s, ServiceInfo) for s in services)
        assert services[0].fqn == "EMR.SVC.HDB.1"
        assert services[1].fqn == "EMR.SVC.HDB.2"


class TestQNSEngine:
    @patch("qorm.qns.resolve_services")
    @patch("qorm.qns.load_registry_nodes")
    def test_returns_engine(
        self, mock_load: MagicMock, mock_resolve: MagicMock
    ) -> None:
        mock_load.return_value = [MagicMock()]
        mock_resolve.return_value = _make_service_rows("EMR.SVC.HDB.1")
        qns = QNS(market="fx", env="prod", username="u", password="p")
        engine = qns.engine("EMR.SVC.HDB.1")
        assert isinstance(engine, Engine)
        assert engine.host == "host0"
        assert engine.port == 5010
        assert engine.tls is True
        assert engine.username == "u"
        assert engine.password == "p"

    @patch("qorm.qns.resolve_services")
    @patch("qorm.qns.load_registry_nodes")
    def test_not_found_raises(
        self, mock_load: MagicMock, mock_resolve: MagicMock
    ) -> None:
        mock_load.return_value = [MagicMock()]
        mock_resolve.return_value = _make_service_rows("OTHER.SVC.HDB.1")
        qns = QNS(market="fx", env="prod")
        with pytest.raises(QNSServiceNotFoundError, match="not found"):
            qns.engine("EMR.SVC.HDB.1")

    def test_bad_name_format_raises(self) -> None:
        with patch("qorm.qns.load_registry_nodes", return_value=[MagicMock()]):
            qns = QNS(market="fx", env="prod")
        with pytest.raises(QNSConfigError, match="DATASET.CLUSTER.DBTYPE.NODE"):
            qns.engine("ONLY.TWO")


class TestQNSEngines:
    @patch("qorm.qns.resolve_services")
    @patch("qorm.qns.load_registry_nodes")
    def test_returns_multiple_engines(
        self, mock_load: MagicMock, mock_resolve: MagicMock
    ) -> None:
        mock_load.return_value = [MagicMock()]
        mock_resolve.return_value = _make_service_rows(
            "EMR.SVC.HDB.1", "EMR.SVC.HDB.2", "EMR.SVC.HDB.3"
        )
        qns = QNS(market="fx", env="prod")
        engines = qns.engines("EMR", "SVC", "HDB")
        assert len(engines) == 3
        assert all(isinstance(e, Engine) for e in engines)
        assert engines[0].port == 5010
        assert engines[2].port == 5012


class TestEngineFromService:
    @patch("qorm.qns.resolve_services")
    @patch("qorm.qns.load_registry_nodes")
    def test_delegates_to_qns(
        self, mock_load: MagicMock, mock_resolve: MagicMock
    ) -> None:
        mock_load.return_value = [MagicMock()]
        mock_resolve.return_value = _make_service_rows("EMR.SVC.HDB.1")
        engine = Engine.from_service(
            "EMR.SVC.HDB.1", market="fx", env="prod",
            username="u", password="p",
        )
        assert isinstance(engine, Engine)
        assert engine.host == "host0"
        assert engine.port == 5010
