"""Unit tests for IPC compression/decompression."""

import struct

import pytest

from qorm.protocol.compress import compress, decompress
from qorm.protocol.constants import LITTLE_ENDIAN, RESPONSE_MSG, HEADER_SIZE


class TestCompression:
    def test_no_compress_short_data(self):
        """Data shorter than 32 bytes should not be compressed."""
        data = b"short"
        result = compress(data, level=1)
        assert result == data

    def test_no_compress_level_zero(self):
        """Level 0 means no compression."""
        data = b"x" * 100
        result = compress(data, level=0)
        assert result == data

    def test_compress_returns_bytes(self):
        data = b"ABCABC" * 100
        result = compress(data, level=1)
        assert isinstance(result, bytes)

    def test_decompress_returns_bytes(self):
        data = b"hello world"
        result = decompress(data)
        assert isinstance(result, bytes)

    def test_decompress_short_data(self):
        """Very short data passes through."""
        data = b"hi"
        result = decompress(data)
        assert result == data


class TestCompressionEdgeCases:
    def test_empty_data_compress(self):
        result = compress(b"", level=1)
        assert result == b""

    def test_empty_data_decompress(self):
        result = decompress(b"")
        assert result == b""

    def test_single_byte(self):
        result = compress(b"x", level=1)
        assert result == b"x"

    def test_all_same_bytes(self):
        """Highly compressible data."""
        data = b"\x00" * 1000
        compressed = compress(data, level=1)
        # Should either be smaller or return original
        assert isinstance(compressed, bytes)

    def test_random_like_data(self):
        """Data with no patterns may not compress well."""
        import hashlib
        data = hashlib.sha256(b"seed").digest() * 10  # 320 bytes, not very compressible
        result = compress(data, level=1)
        assert isinstance(result, bytes)


class TestCompressDecompressRoundTrip:
    def test_round_trip_recovers_original(self):
        """Compress then decompress should recover the original IPC message."""
        body = b"ABCDEFGH" * 100  # 800 bytes, highly compressible
        total_len = HEADER_SIZE + len(body)
        header = struct.pack('<BBHi', LITTLE_ENDIAN, RESPONSE_MSG, 0, total_len)
        original = header + body

        compressed = compress(original, level=1)
        assert compressed != original, "data should actually compress"
        decompressed = decompress(compressed)
        assert decompressed == original

    def test_round_trip_with_varied_payload(self):
        """Round-trip with a payload containing mixed repeated patterns."""
        # Simulate a serialized table-like payload
        pattern = b"\x00\x01\x02\x03" * 50 + b"trade\x00" * 80
        total_len = HEADER_SIZE + len(pattern)
        header = struct.pack('<BBHi', LITTLE_ENDIAN, RESPONSE_MSG, 0, total_len)
        original = header + pattern

        compressed = compress(original, level=1)
        if compressed != original:
            decompressed = decompress(compressed)
            assert decompressed == original
