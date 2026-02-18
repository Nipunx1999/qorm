"""Tests for qorm.qns._registry — CSV loading."""

import textwrap
from pathlib import Path

import pytest

from qorm.exc import QNSConfigError
from qorm.qns._registry import RegistryNode, load_registry_nodes


VALID_CSV = textwrap.dedent("""\
    dataset,cluster,dbtype,node,host,port,port_env,env
    EMRATESCV,SERVICE,HDB,1,host1.example.com,5010,QNS_PORT,prod
    EMRATESCV,SERVICE,HDB,2,host2.example.com,5011,QNS_PORT,prod
""")


def _write_csv(tmp_path: Path, market: str, env: str, content: str) -> Path:
    csv_file = tmp_path / f"{market}_{env}.csv"
    csv_file.write_text(content, encoding="utf-8")
    return csv_file


class TestCSVFilenameGeneration:
    def test_lowercase_filename(self, tmp_path: Path) -> None:
        _write_csv(tmp_path, "fx", "prod", VALID_CSV)
        nodes = load_registry_nodes("FX", "PROD", data_dir=tmp_path)
        assert len(nodes) == 2

    def test_mixed_case_filename(self, tmp_path: Path) -> None:
        _write_csv(tmp_path, "fx", "uat", VALID_CSV)
        nodes = load_registry_nodes("Fx", "Uat", data_dir=tmp_path)
        assert len(nodes) == 2


class TestLoadValidCSV:
    def test_correct_node_objects(self, tmp_path: Path) -> None:
        _write_csv(tmp_path, "fx", "prod", VALID_CSV)
        nodes = load_registry_nodes("fx", "prod", data_dir=tmp_path)
        assert len(nodes) == 2
        assert nodes[0] == RegistryNode(
            dataset="EMRATESCV",
            cluster="SERVICE",
            dbtype="HDB",
            node="1",
            host="host1.example.com",
            port=5010,
            port_env="QNS_PORT",
            env="prod",
        )
        assert nodes[1].host == "host2.example.com"
        assert nodes[1].port == 5011


class TestMissingFile:
    def test_raises_config_error(self, tmp_path: Path) -> None:
        with pytest.raises(QNSConfigError, match="not found"):
            load_registry_nodes("fx", "prod", data_dir=tmp_path)


class TestEmptyCSV:
    def test_headers_only_raises(self, tmp_path: Path) -> None:
        _write_csv(tmp_path, "fx", "prod",
                    "dataset,cluster,dbtype,node,host,port,port_env,env\n")
        with pytest.raises(QNSConfigError, match="no data rows"):
            load_registry_nodes("fx", "prod", data_dir=tmp_path)


class TestMalformedRow:
    def test_bad_port_raises(self, tmp_path: Path) -> None:
        csv = textwrap.dedent("""\
            dataset,cluster,dbtype,node,host,port,port_env,env
            EMR,SVC,HDB,1,host,NOT_A_NUMBER,QNS,prod
        """)
        _write_csv(tmp_path, "fx", "prod", csv)
        with pytest.raises(QNSConfigError, match="Malformed row"):
            load_registry_nodes("fx", "prod", data_dir=tmp_path)


class TestWhitespaceStripped:
    def test_fields_trimmed(self, tmp_path: Path) -> None:
        csv = textwrap.dedent("""\
            dataset,cluster,dbtype,node,host,port,port_env,env
             EMRATESCV , SERVICE , HDB , 1 , host1.example.com , 5010 , QNS_PORT , prod
        """)
        _write_csv(tmp_path, "fx", "prod", csv)
        nodes = load_registry_nodes("fx", "prod", data_dir=tmp_path)
        assert nodes[0].dataset == "EMRATESCV"
        assert nodes[0].host == "host1.example.com"
        assert nodes[0].port == 5010


class TestMultipleNodes:
    def test_three_nodes(self, tmp_path: Path) -> None:
        csv = textwrap.dedent("""\
            dataset,cluster,dbtype,node,host,port,port_env,env
            A,B,C,1,h1,5001,P,prod
            D,E,F,2,h2,5002,P,prod
            G,H,I,3,h3,5003,P,prod
        """)
        _write_csv(tmp_path, "fx", "prod", csv)
        nodes = load_registry_nodes("fx", "prod", data_dir=tmp_path)
        assert len(nodes) == 3
        assert [n.dataset for n in nodes] == ["A", "D", "G"]
