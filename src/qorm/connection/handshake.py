"""IPC handshake and capability negotiation for q/kdb+.

The kdb+ IPC handshake works as follows:
1. Client sends: "username:password\\x03\\x00" (capability byte 3 = compression support)
   or "username:password\\x00" for basic connection
2. Server responds with a single byte indicating accepted capability level
"""

from __future__ import annotations

from ..exc import HandshakeError, AuthenticationError


def build_handshake(username: str = "", password: str = "",
                    capability: int = 3) -> bytes:
    """Build the handshake bytes to send to kdb+.

    Parameters
    ----------
    username : str
        Username (empty string for no auth).
    password : str
        Password (empty string for no auth).
    capability : int
        IPC capability version (0-6). 3 is a safe default.

    Returns
    -------
    bytes
        The handshake payload to send.
    """
    cred = f"{username}:{password}" if username or password else ""
    return cred.encode('utf-8') + bytes([capability, 0])


def parse_handshake_response(data: bytes) -> int:
    """Parse the server's handshake response.

    Parameters
    ----------
    data : bytes
        Raw bytes received from the server after sending handshake.

    Returns
    -------
    int
        The capability level accepted by the server.

    Raises
    ------
    AuthenticationError
        If the server rejected the connection (empty response).
    HandshakeError
        If the response is malformed.
    """
    if not data:
        raise AuthenticationError("Connection rejected by kdb+ (empty response)")
    if len(data) == 1:
        return data[0]
    raise HandshakeError(f"Unexpected handshake response length: {len(data)}")
