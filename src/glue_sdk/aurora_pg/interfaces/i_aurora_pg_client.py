from abc import ABC, abstractmethod
from typing import Optional, Literal,TYPE_CHECKING
from glue_sdk.interfaces.i_client import IClient

if TYPE_CHECKING:
    import psycopg2.extensions as psycopg
    from glue_sdk.interfaces.i_client import IClient
    from psycopg2.extensions import connection as psycopg_connection
    
class IAuroraPgClient(IClient,ABC):
    """
    Abstract base class for Aurora PostgreSQL Client.
    """

    @property
    @abstractmethod
    def db_host(self) -> str:
        """Aurora PostgreSQL cluster endpoint."""
        pass

    @property
    @abstractmethod
    def db_port(self) -> int:
        """Port number for the database."""
        pass

    @property
    @abstractmethod
    def db_name(self) -> str:
        """Name of the database to connect to."""
        pass

    @property
    @abstractmethod
    def connection_name(self) -> Optional[str]:
        """Glue connection name."""
        pass

    @property
    @abstractmethod
    def sslmode(self) -> Literal['require', 'disable']:
        """SSL mode for the connection."""
        pass

    @property
    @abstractmethod
    def ssl_root_cert(self) -> Optional[str]:
        """Path to the SSL root certificate."""
        pass

    @property
    @abstractmethod
    def connection(self) -> Optional["psycopg_connection"]:
        """Active database connection."""
        pass

    @abstractmethod
    def create_client(self) -> "psycopg.connection":
        """
        Establishes and retrieves a connection to the Aurora PostgreSQL database.

        Returns:
            Optional[psycopg.connection]: Active database connection or None.
        """
        pass

    @abstractmethod
    def create_connection(self) -> Optional["psycopg.connection"]:
        """
        Establishes a new connection to the Aurora PostgreSQL database.

        Returns:
            Optional[psycopg.connection]: New database connection or None if unsuccessful.
        """
        pass

    @abstractmethod
    def fetch_version(self) -> None:
        """
        Logs the PostgreSQL database version.
        """
        pass

    @abstractmethod
    def close_connection(self) -> None:
        """
        Closes the database connection if it is open.
        """
        pass

    @abstractmethod
    def __enter__(self) -> 'IAuroraPgClient':
        """
        Enter the runtime context for the client.

        Returns:
            IAuroraPgClient: Instance of the client with an active connection.
        """
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the runtime context and close the connection.

        Args:
            exc_type: Type of exception.
            exc_val: Value of exception.
            exc_tb: Traceback object.
        """
        pass

    @abstractmethod
    def __del__(self):
        """
        Ensures the connection is closed when the instance is deleted.
        """
        pass