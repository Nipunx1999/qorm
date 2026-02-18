"""IPC compression for q/kdb+.

kdb+ IPC uses a custom compression algorithm based on hash-table lookups
of 2-byte patterns.  The compressed format has:
- 8 bytes: uncompressed length (little-endian int)
- Remaining: compressed data stream with control bytes and literal/reference ops

When the IPC header byte 2 is set, the payload after the header is compressed.
The uncompressed data includes the original 8-byte header.
"""

from __future__ import annotations

import struct


def compress(data: bytes, level: int = 0) -> bytes:
    """Compress an IPC payload using q's compression algorithm.

    Parameters
    ----------
    data : bytes
        The raw IPC message (including 8-byte header) to compress.
    level : int
        Compression level hint (0 = no compression, >0 = compress).
        Level is primarily a flag; actual compression uses the standard
        kdb+ algorithm.

    Returns
    -------
    bytes
        Compressed payload (or original data if compression isn't beneficial).
    """
    if level <= 0 or len(data) < 32:
        return data

    n = len(data)
    # Hash table for pattern matching (12-bit hash → position)
    hash_table = [0] * 4096
    compressed = bytearray()

    # Compressed header: original uncompressed length
    compressed.extend(struct.pack('<i', n))

    src = 0
    ctrl_pos = len(compressed)
    compressed.append(0)  # control byte placeholder
    ctrl_bit = 0
    ctrl_byte = 0

    dst_start = len(compressed)

    while src < n:
        if ctrl_bit == 8:
            compressed[ctrl_pos] = ctrl_byte
            ctrl_pos = len(compressed)
            compressed.append(0)
            ctrl_bit = 0
            ctrl_byte = 0

        if src + 1 < n:
            h = ((data[src] ^ data[src + 1]) * 257) & 0xFFF
            ref = hash_table[h]
            hash_table[h] = src

            if (ref > 0 and ref < src and src - ref < 32768
                    and ref + 2 < n and data[ref] == data[src]
                    and data[ref + 1] == data[src + 1]):
                # Find match length (min 2, max 255+2)
                match_len = 2
                max_match = min(257, n - src)
                while match_len < max_match and data[ref + match_len] == data[src + match_len]:
                    match_len += 1

                offset = src - ref
                # Encode as back-reference
                if match_len <= 3 and offset <= 255:
                    compressed.append(offset & 0xFF)
                    compressed.append(((match_len - 2) << 4) | 0)
                else:
                    compressed.append(offset & 0xFF)
                    compressed.append(((offset >> 8) & 0x7F) | 0x80)
                    compressed.append(match_len - 2)

                src += match_len
                ctrl_byte |= (1 << ctrl_bit)
            else:
                # Literal byte
                compressed.append(data[src])
                src += 1
        else:
            compressed.append(data[src])
            src += 1

        ctrl_bit += 1

    compressed[ctrl_pos] = ctrl_byte

    # Only use compressed version if it's actually smaller
    if len(compressed) + 8 >= n:
        return data

    return bytes(compressed)


def decompress(data: bytes, header_offset: int = 0) -> bytes:
    """Decompress a q IPC compressed payload.

    Parameters
    ----------
    data : bytes
        The compressed payload (after the 8-byte IPC header).
    header_offset : int
        Offset into data where compression header starts (default 0).

    Returns
    -------
    bytes
        The decompressed payload.
    """
    if len(data) < 4:
        return data

    pos = header_offset
    uncompressed_len = struct.unpack_from('<i', data, pos)[0]
    pos += 4

    output = bytearray(uncompressed_len)
    out_pos = 0

    while pos < len(data) and out_pos < uncompressed_len:
        ctrl = data[pos]
        pos += 1

        for bit in range(8):
            if pos >= len(data) or out_pos >= uncompressed_len:
                break

            if ctrl & (1 << bit):
                # Back-reference
                if pos + 1 >= len(data):
                    break
                b0 = data[pos]
                b1 = data[pos + 1]
                pos += 2

                if b1 & 0x80:
                    # Long form
                    if pos >= len(data):
                        break
                    offset = b0 | ((b1 & 0x7F) << 8)
                    length = data[pos] + 2
                    pos += 1
                else:
                    # Short form
                    offset = b0
                    length = ((b1 >> 4) & 0x0F) + 2

                ref_pos = out_pos - offset
                for i in range(length):
                    if out_pos >= uncompressed_len:
                        break
                    output[out_pos] = output[ref_pos + i]
                    out_pos += 1
            else:
                # Literal byte
                output[out_pos] = data[pos]
                out_pos += 1
                pos += 1

    return bytes(output[:out_pos])
