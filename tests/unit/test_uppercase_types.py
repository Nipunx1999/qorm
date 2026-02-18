"""Unit tests for uppercase type char support in reflection."""

import pytest

from qorm.model.reflect import (
    _CHAR_TO_QTYPE_CODE,
    _UPPER_CHAR_TO_QTYPE_CODE,
    _resolve_type_char,
    build_model_from_meta,
)
from qorm.protocol.constants import QTypeCode


class TestUppercaseTypeChars:
    def test_uppercase_map_exists(self):
        """All lowercase type chars (except space) have uppercase equivalents."""
        for ch in _CHAR_TO_QTYPE_CODE:
            if ch == ' ':
                continue
            assert ch.upper() in _UPPER_CHAR_TO_QTYPE_CODE

    def test_uppercase_maps_to_mixed_list(self):
        """All uppercase type chars map to MIXED_LIST."""
        for ch, code in _UPPER_CHAR_TO_QTYPE_CODE.items():
            assert code == QTypeCode.MIXED_LIST

    def test_resolve_lowercase(self):
        assert _resolve_type_char('j') == QTypeCode.LONG
        assert _resolve_type_char('f') == QTypeCode.FLOAT
        assert _resolve_type_char('s') == QTypeCode.SYMBOL

    def test_resolve_uppercase(self):
        assert _resolve_type_char('J') == QTypeCode.MIXED_LIST
        assert _resolve_type_char('F') == QTypeCode.MIXED_LIST
        assert _resolve_type_char('C') == QTypeCode.MIXED_LIST

    def test_resolve_unknown(self):
        assert _resolve_type_char('?') is None
        assert _resolve_type_char('!') is None

    def test_resolve_space(self):
        assert _resolve_type_char(' ') == QTypeCode.MIXED_LIST

    def test_build_model_with_uppercase_char(self):
        """Table with a 'C' column (list of strings) should work."""
        meta = {
            'c': ['sym', 'labels'],
            't': ['s', 'C'],
            'f': ['', ''],
            'a': ['', ''],
        }
        M = build_model_from_meta('tagged', meta)
        assert M.__fields__['labels'].qtype.code == QTypeCode.MIXED_LIST
        assert M.__fields__['sym'].qtype.code == QTypeCode.SYMBOL

    def test_build_model_with_J_column(self):
        """Table with a 'J' column (list of long vectors)."""
        meta = {
            'c': ['id', 'nested_ids'],
            't': ['j', 'J'],
            'f': ['', ''],
            'a': ['', ''],
        }
        M = build_model_from_meta('nested_tbl', meta)
        assert M.__fields__['id'].qtype.code == QTypeCode.LONG
        assert M.__fields__['nested_ids'].qtype.code == QTypeCode.MIXED_LIST

    def test_all_uppercase_chars_valid(self):
        """Every uppercase type char can be used to build a model."""
        for ch in _UPPER_CHAR_TO_QTYPE_CODE:
            meta = {'c': ['col'], 't': [ch], 'f': [''], 'a': ['']}
            M = build_model_from_meta(f'tbl_{ch}', meta)
            assert M.__fields__['col'].qtype.code == QTypeCode.MIXED_LIST
