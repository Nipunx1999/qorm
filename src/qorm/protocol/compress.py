"""IPC compression for q/kdb+.

kdb+ IPC uses a custom LZ-style compression algorithm with a 256-entry
hash table keyed by XOR of consecutive byte pairs.

Compressed payload format (after the 8-byte IPC header):
- Bytes 0-3: uncompressed total length (little-endian int32),
  includes the original 8-byte IPC header
- Bytes 4-7: initial hash cursor position (little-endian int32)
- Bytes 8+:  compressed bitstream

The decompressed output positions 0-7 correspond to the original
IPC header and must be reconstructed by the caller or via the
``header_bytes`` parameter.

Algorithm based on the kx c.java reference implementation.
"""

from __future__ import annotations

import struct


def compress(data: bytes, level: int = 0) -> bytes:
    """Compress an IPC message using the kdb+ compression algorithm.

    Parameters
    ----------
    data : bytes
        The raw IPC message (including 8-byte header) to compress.
    level : int
        Compression level hint (0 = no compression, >0 = compress).

    Returns
    -------
    bytes
        Compressed payload suitable for sending after a compressed IPC
        header, or the original *data* if compression is not beneficial.
    """
    if level <= 0 or len(data) <= 17:
        return data

    n = len(data)
    aa = [0] * 256  # hash table: XOR hash → position in data

    s = 8   # source position in input (skip header)
    d = s   # hash cursor — starts at s to skip header bytes

    out = bytearray()

    # 8-byte compression sub-header
    out.extend(struct.pack('<i', n))  # uncompressed total length
    out.extend(struct.pack('<i', d))  # initial hash cursor position
    i = 0   # bit multiplier (0 means need fresh control byte)
    f = 0   # control byte accumulator
    f_pos = len(out)
    out.append(0)  # control byte placeholder

    while s < n:
        if i == 256:
            out[f_pos] = f
            f_pos = len(out)
            out.append(0)
            i = 1
            f = 0
        elif i == 0:
            i = 1

        # Update hash table up to (but not including) last byte before s
        while d + 1 < s and d + 1 < n:
            aa[(data[d] ^ data[d + 1]) & 0xFF] = d
            d += 1

        # Try to find a back-reference
        matched = False
        if s + 1 < n:
            h = (data[s] ^ data[s + 1]) & 0xFF
            r = aa[h]
            if r > 0 and r + 1 < n and data[r] == data[s] and data[r + 1] == data[s + 1]:
                # Determine match length (min 2, max 257)
                match_len = 2
                max_len = min(257, n - s, n - r)
                while match_len < max_len and data[r + match_len] == data[s + match_len]:
                    match_len += 1

                # Encode back-reference: [hash_index, extra_length]
                f |= i
                out.append(h)
                out.append(match_len - 2)

                # Advance: copy 2 bytes worth of hash updates, skip the rest
                old_s = s
                s += 2
                # Hash updates for the 2 explicitly-copied bytes
                while d + 1 < s and d + 1 < n:
                    aa[(data[d] ^ data[d + 1]) & 0xFF] = d
                    d += 1
                # Skip hash updates for the remaining matched bytes
                s = old_s + match_len
                d = s
                matched = True

        if not matched:
            # Literal byte
            out.append(data[s])
            s += 1

        i *= 2

    out[f_pos] = f

    # Only use compressed version if it's actually smaller
    if len(out) >= n:
        return data

    return bytes(out)


def decompress(data: bytes, header_bytes: bytes = b'') -> bytes:
    """Decompress a kdb+ IPC compressed payload.

    Parameters
    ----------
    data : bytes
        The compressed payload (everything after the 8-byte IPC header).
    header_bytes : bytes, optional
        The 8-byte IPC header from the compressed message.  Used to
        reconstruct the original (uncompressed) header in positions 0-7
        of the output.  If empty, positions 0-7 will be zeroed.

    Returns
    -------
    bytes
        The full decompressed IPC message including the 8-byte header.
    """
    if len(data) < 8:
        return data

    # Compression sub-header: bytes 0-3 = uncompressed total length,
    # bytes 4-7 are unused padding (kdb+ writes 0 there).
    uncompressed_len = struct.unpack_from('<i', data, 0)[0]

    dst = bytearray(uncompressed_len)
    aa = [0] * 256  # hash table: XOR hash → position in output

    # Pre-fill the original IPC header in positions 0-7 BEFORE
    # decompressing.  The kdb+ compressor can emit back-references
    # that point into the header (aa[h] defaults to 0), so these
    # bytes must be correct before the decompression loop runs.
    if len(header_bytes) >= 8:
        dst[0] = header_bytes[0]        # endian
        dst[1] = header_bytes[1]        # msg_type
        dst[2] = 0                      # not compressed
        dst[3] = 0                      # reserved
        struct.pack_into('<i', dst, 4, uncompressed_len)

    s = 8  # output position (positions 0-7 are the header)
    p = 8  # input position (skip compression sub-header)
    d = s  # hash cursor — always starts at s, matching c.java reference
    i = 0  # bit multiplier (0 means need fresh control byte)
    f = 0  # control byte

    while s < uncompressed_len and p < len(data):
        if i == 0:
            f = data[p]
            p += 1
            i = 1

        if f & i:
            # Back-reference: look up hash table
            r = aa[data[p] & 0xFF]
            p += 1
            dst[s] = dst[r]
            s += 1
            r += 1
            dst[s] = dst[r]
            s += 1
            r += 1
            n = data[p] & 0xFF
            p += 1
            for m in range(n):
                dst[s + m] = dst[r + m]
        else:
            # Literal byte
            dst[s] = data[p]
            s += 1
            p += 1

        # Update hash table for new output bytes
        while d + 1 < s and d + 1 < uncompressed_len:
            aa[(dst[d] ^ dst[d + 1]) & 0xFF] = d
            d += 1

        if f & i:
            # Finish back-reference: advance past extra copied bytes
            s += n
            d = s

        i *= 2
        if i == 256:
            i = 0

    return bytes(dst)
