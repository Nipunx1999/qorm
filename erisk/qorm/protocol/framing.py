"""IPC message framing: 8-byte header pack/unpack."""

from __future__ import annotations

import struct

from .constants import HEADER_SIZE, LITTLE_ENDIAN

# Header layout: [endian: 1B][msg_type: 1B][reserved: 2B][length: 4I]
_HEADER_LE = struct.Struct('<BBHi')  # little-endian
_HEADER_BE = struct.Struct('>BBHi')  # big-endian


def pack_header(msg_type: int, payload_length: int) -> bytes:
    """Pack an 8-byte IPC header (always little-endian).

    Parameters
    ----------
    msg_type : int
        Message type (0=async, 1=sync, 2=response).
    payload_length : int
        Total message length including the 8-byte header.
    """
    return _HEADER_LE.pack(LITTLE_ENDIAN, msg_type, 0, payload_length)


def unpack_header(data: bytes | bytearray | memoryview) -> tuple[int, int, int]:
    """Unpack an 8-byte IPC header.

    Returns
    -------
    (endian, msg_type, total_length)
    """
    if len(data) < HEADER_SIZE:
        raise ValueError(f"Header too short: {len(data)} < {HEADER_SIZE}")
    endian = data[0]
    s = _HEADER_LE if endian == LITTLE_ENDIAN else _HEADER_BE
    endian_b, msg_type, _, total_length = s.unpack(bytes(data[:HEADER_SIZE]))
    return endian_b, msg_type, total_length
