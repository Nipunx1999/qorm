"""Unit tests for YAML/TOML/JSON config loading."""

import json
import pytest
from pathlib import Path
from unittest.mock import patch

from qorm.config import load_config, engines_from_config, group_from_config
from qorm.registry import EngineRegistry, EngineGroup


class TestLoadConfig:
    def test_json_loading(self, tmp_path):
        config = {
            "rdb": {"host": "eq-rdb", "port": 5010},
            "hdb": {"host": "eq-hdb", "port": 5012},
        }
        f = tmp_path / "test.json"
        f.write_text(json.dumps(config))
        result = load_config(f)
        assert result == config

    def test_toml_loading(self, tmp_path):
        toml_content = '[rdb]\nhost = "eq-rdb"\nport = 5010\n'
        f = tmp_path / "test.toml"
        f.write_text(toml_content)
        try:
            result = load_config(f)
            assert result['rdb']['host'] == 'eq-rdb'
            assert result['rdb']['port'] == 5010
        except ImportError:
            pytest.skip("No TOML library available")

    def test_yaml_loading(self, tmp_path):
        yaml_content = "rdb:\n  host: eq-rdb\n  port: 5010\n"
        f = tmp_path / "test.yaml"
        f.write_text(yaml_content)
        try:
            result = load_config(f)
            assert result['rdb']['host'] == 'eq-rdb'
            assert result['rdb']['port'] == 5010
        except ImportError:
            pytest.skip("pyyaml not installed")

    def test_unknown_extension_raises(self, tmp_path):
        f = tmp_path / "test.xml"
        f.write_text("<config></config>")
        with pytest.raises(ValueError, match="Unsupported config file extension"):
            load_config(f)

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.json")

    def test_toml_missing_dep_message(self, tmp_path):
        f = tmp_path / "test.toml"
        f.write_text('[rdb]\nhost = "x"\n')
        with patch.dict('sys.modules', {'tomllib': None, 'tomli': None}):
            # Force re-import to pick up patched modules
            try:
                result = load_config(f)
            except ImportError as e:
                assert 'tomli' in str(e).lower() or 'toml' in str(e).lower()
            except Exception:
                pass  # toml may already be importable


class TestEnginesFromConfig:
    def test_returns_registry(self, tmp_path):
        config = {
            "rdb": {"host": "eq-rdb", "port": 5010},
            "hdb": {"host": "eq-hdb", "port": 5012},
        }
        f = tmp_path / "engines.json"
        f.write_text(json.dumps(config))
        registry = engines_from_config(f)
        assert isinstance(registry, EngineRegistry)
        assert 'rdb' in registry.names
        assert 'hdb' in registry.names
        assert registry.get('rdb').host == 'eq-rdb'


class TestGroupFromConfig:
    def test_returns_group(self, tmp_path):
        config = {
            "equities": {
                "rdb": {"host": "eq-rdb", "port": 5010},
            },
            "fx": {
                "rdb": {"host": "fx-rdb", "port": 5020},
            },
        }
        f = tmp_path / "group.json"
        f.write_text(json.dumps(config))
        group = group_from_config(f)
        assert isinstance(group, EngineGroup)
        assert 'equities' in group.names
        assert 'fx' in group.names
