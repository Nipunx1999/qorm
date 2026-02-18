"""Unit tests for IPC compression/decompression."""

import struct

import pytest

from qorm.protocol.compress import compress, decompress
from qorm.protocol.constants import LITTLE_ENDIAN, RESPONSE_MSG, HEADER_SIZE


class TestCompression:
    def test_no_compress_short_data(self):
        """Data shorter than 18 bytes should not be compressed."""
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
        data = b"hello"
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
        assert isinstance(compressed, bytes)

    def test_random_like_data(self):
        """Data with no patterns may not compress well."""
        import hashlib
        data = hashlib.sha256(b"seed").digest() * 10  # 320 bytes
        result = compress(data, level=1)
        assert isinstance(result, bytes)


def _make_ipc_message(body: bytes) -> tuple[bytes, bytes]:
    """Build an IPC message and return (header, full_message)."""
    total_len = HEADER_SIZE + len(body)
    header = struct.pack('<BBHi', LITTLE_ENDIAN, RESPONSE_MSG, 0, total_len)
    return header, header + body


class TestCompressDecompressRoundTrip:
    def test_round_trip_recovers_original(self):
        """Compress then decompress should recover the original IPC message."""
        body = b"ABCDEFGH" * 100  # 800 bytes, highly compressible
        header, original = _make_ipc_message(body)

        compressed = compress(original, level=1)
        assert compressed != original, "data should actually compress"
        decompressed = decompress(compressed, header)
        assert decompressed == original

    def test_round_trip_with_varied_payload(self):
        """Round-trip with a payload containing mixed repeated patterns."""
        pattern = b"\x00\x01\x02\x03" * 50 + b"trade\x00" * 80
        header, original = _make_ipc_message(pattern)

        compressed = compress(original, level=1)
        if compressed != original:
            decompressed = decompress(compressed, header)
            assert decompressed == original

    def test_round_trip_large_payload(self):
        """Round-trip with a larger payload simulating a table response."""
        rows = b"".join(
            struct.pack('<q', i) + b"AAPL\x00" + struct.pack('<d', 150.0 + i * 0.01)
            for i in range(200)
        )
        header, original = _make_ipc_message(rows)

        compressed = compress(original, level=1)
        if compressed != original:
            decompressed = decompress(compressed, header)
            assert decompressed == original

    def test_back_reference_into_header(self):
        """kdb+ compressor can back-reference position 0 (the header).

        When aa[h] defaults to 0 and the header bytes match payload bytes,
        the compressor encodes a back-reference into the header region.
        The decompressor must have the header pre-filled before the loop.
        """
        # Build a body whose first bytes match the IPC header bytes.
        # The header starts with LITTLE_ENDIAN (0x01), RESPONSE_MSG (0x02),
        # then 0x00, 0x00.  Make the body start with the same pattern so
        # that a real kdb+ compressor would back-reference position 0.
        header_echo = struct.pack('<BBH', LITTLE_ENDIAN, RESPONSE_MSG, 0)
        body = header_echo * 200  # 800 bytes, repeating the header prefix
        header, original = _make_ipc_message(body)

        compressed = compress(original, level=1)
        if compressed != original:
            decompressed = decompress(compressed, header)
            assert decompressed == original

    def test_header_reconstruction(self):
        """Decompressed output should have the correct IPC header."""
        body = b"XYZXYZ" * 100
        header, original = _make_ipc_message(body)

        compressed = compress(original, level=1)
        assert compressed != original

        # Pass header with compressed flag set (as in real receive path)
        compressed_header = struct.pack(
            '<BBHi', LITTLE_ENDIAN, RESPONSE_MSG, 1, len(compressed) + HEADER_SIZE,
        )
        decompressed = decompress(compressed, compressed_header)

        # Positions 0-1 should match (endian, msg_type)
        assert decompressed[0] == LITTLE_ENDIAN
        assert decompressed[1] == RESPONSE_MSG
        # Position 2 should be 0 (decompressed = not compressed)
        assert decompressed[2] == 0
        # Positions 4-7 should be the uncompressed total length
        total = struct.unpack_from('<i', decompressed, 4)[0]
        assert total == len(original)
        # Body should match
        assert decompressed[8:] == original[8:]
