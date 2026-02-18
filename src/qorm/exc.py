"""Exception hierarchy for qorm."""


class QormError(Exception):
    """Base exception for all qorm errors."""


class ConnectionError(QormError):
    """Failed to connect to kdb+ process."""


class HandshakeError(ConnectionError):
    """IPC handshake failed."""


class AuthenticationError(HandshakeError):
    """Authentication rejected by kdb+ process."""


class SerializationError(QormError):
    """Failed to serialize Python object to q binary."""


class DeserializationError(QormError):
    """Failed to deserialize q binary to Python object."""


class QueryError(QormError):
    """Error executing a query against kdb+."""


class QError(QueryError):
    """Error returned by the kdb+ process (q-level error)."""

    def __init__(self, message: str) -> None:
        self.q_message = message
        super().__init__(f"q error: {message}")


class ModelError(QormError):
    """Error in model definition or usage."""


class SchemaError(QormError):
    """Error in DDL operations."""


class PoolError(ConnectionError):
    """Connection pool error."""


class PoolExhaustedError(PoolError):
    """No connections available in pool."""


class EngineNotFoundError(QormError):
    """Named engine not found in a registry or group."""


class ReflectionError(QormError):
    """Error reflecting table metadata from kdb+."""


class QNSError(QormError):
    """Base exception for QNS (Q Name Service) errors."""


class QNSConfigError(QNSError):
    """CSV missing, empty, malformed, or bad service name format."""


class QNSRegistryError(QNSError):
    """All registry nodes unreachable."""


class QNSServiceNotFoundError(QNSError):
    """Service not found in registry results."""
