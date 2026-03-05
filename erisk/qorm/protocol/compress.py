"""IPC compression for q/kdb+.

kdb+ IPC uses a custom LZ-style compression algorithm with a 256-entry
hash table keyed by XOR of consecutive byte pairs.

Compressed payload format (after the 8-byte IPC header):
- Bytes 0-3: uncompressed total length (int32, endianness matches
  the IPC header's endian byte)
- Bytes 4+:  compressed bitstream

The decompressed output positions 0-7 correspond to the original
IPC header and must be reconstructed by the caller or via the
``header_bytes`` parameter.

Algorithm faithfully ported from the kx c.java reference implementation.
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
        Compressed payload (4-byte origSize header + bitstream) suitable
        for sending after a compressed IPC header, or the original *data*
        if compression is not beneficial.
    """
    if level <= 0 or len(data) <= 17:
        return data

    y = data
    t = len(y)          # source end = origSize (includes 8-byte IPC header)

    # Output buffer: half the original size.
    # If compressed output approaches this limit, abort (matching c.java).
    e = t // 2
    if e < 22:
        return data
    out = bytearray(e)

    # Write origSize at position 0-3 (little-endian, matching our LE output)
    struct.pack_into('<i', out, 0, t)

    c = 4       # control byte position in output
    d = c       # output write cursor
    s = 8       # source cursor (skip 8-byte IPC header)
    i = 0       # bit multiplier (simulates Java byte: overflows 128→0 via & 0xFF)
    f = 0       # control byte accumulator
    s0 = 0      # deferred hash update: source position (0 = none pending)
    h0 = 0      # deferred hash update: hash value
    h = 0       # current hash
    p = 0       # match position from hash table
    a = [0] * 256  # hash table: XOR hash → source position

    while s < t:
        # --- control byte management (c.java: if(0==i){...}) ---
        if i == 0:
            if d > e - 17:
                return data  # not enough room — abort compression
            i = 1
            out[c] = f & 0xFF
            c = d
            d += 1
            f = 0

        # --- match check (c.java single-expression with short-circuit) ---
        if s > t - 3:
            # Not enough bytes left for a match; h retains previous value
            g = True
        else:
            h = (y[s] ^ y[s + 1]) & 0xFF
            p = a[h]
            g = (p == 0) or (y[s] != y[p])

        # --- deferred hash update from previous literal ---
        if s0 > 0:
            a[h0] = s0
            s0 = 0

        if g:
            # Literal byte
            h0 = h
            s0 = s
            out[d] = y[s]
            d += 1
            s += 1
        else:
            # Back-reference match
            a[h] = s          # immediate hash update
            f |= i
            p += 2
            r = s + 2
            s += 2
            q = min(s + 255, t)
            while s < q and y[p] == y[s]:
                p += 1
                s += 1
            out[d] = h
            d += 1
            out[d] = s - r
            d += 1

        # --- advance bit counter (simulate Java byte overflow) ---
        i = (i * 2) & 0xFF

    out[c] = f & 0xFF

    # Only use compressed version if it's actually smaller
    if d >= t:
        return data

    return bytes(out[:d])


def decompress(data: bytes, header_bytes: bytes = b'') -> bytes:
    """Decompress a kdb+ IPC compressed payload.

    Parameters
    ----------
    data : bytes
        The compressed payload (everything after the 8-byte IPC header).
        Layout: [origSize: 4 bytes][compressed bitstream].
    header_bytes : bytes, optional
        The 8-byte IPC header from the compressed message.  Used to
        reconstruct the original (uncompressed) header in positions 0-7
        of the output and to determine endianness.  If empty, little-
        endian is assumed and positions 0-7 will be zeroed.

    Returns
    -------
    bytes
        The full decompressed IPC message including the 8-byte header.
    """
    if len(data) < 8:
        return data

    # Determine endianness from header (default little-endian)
    le = len(header_bytes) < 1 or header_bytes[0] == 1
    fmt = '<i' if le else '>i'

    # Sub-header: bytes 0-3 = uncompressed total length
    uncompressed_len = struct.unpack_from(fmt, data, 0)[0]
    if uncompressed_len < 9:
        return data

    dst = bytearray(uncompressed_len)
    aa = [0] * 256  # hash table: XOR hash → position in output

    # Pre-fill the original IPC header in positions 0-7.
    # c.java leaves these as zeros, but our deserializer receives the
    # full message, so correct header bytes are needed.
    if len(header_bytes) >= 8:
        dst[0] = header_bytes[0]        # endian
        dst[1] = header_bytes[1]        # msg_type
        dst[2] = 0                      # not compressed
        dst[3] = 0                      # reserved
        struct.pack_into(fmt, dst, 4, uncompressed_len)

    n = 0   # extra match length
    r = 0   # back-reference position
    f = 0   # control byte
    s = 8   # output position (positions 0-7 are the IPC header)
    p = s   # hash cursor
    i = 0   # bit counter (short in c.java: 0..256)
    d = 4   # input position (compressed stream starts after 4-byte origSize)

    while s < uncompressed_len:
        if i == 0:
            if d >= len(data):
                break
            f = data[d] & 0xFF
            d += 1
            i = 1

        if (f & i) != 0:
            # Back-reference
            r = aa[data[d] & 0xFF]
            d += 1
            dst[s] = dst[r]
            s += 1
            r += 1
            dst[s] = dst[r]
            s += 1
            r += 1
            n = data[d] & 0xFF
            d += 1
            for m in range(n):
                dst[s + m] = dst[r + m]
        else:
            # Literal byte
            if d >= len(data):
                break
            dst[s] = data[d]
            s += 1
            d += 1

        # Update hash table for new output bytes
        while p < s - 1:
            aa[(dst[p] ^ dst[p + 1]) & 0xFF] = p
            p += 1

        if (f & i) != 0:
            # Finish back-reference: advance past extra copied bytes
            s += n
            p = s

        i *= 2
        if i == 256:
            i = 0

    return bytes(dst)
