from typing import Optional, TYPE_CHECKING, Literal
from glue_sdk.core.logger import logger
from glue_sdk.interfaces.i_aurora_pg_client import IAuroraPgClient

if TYPE_CHECKING:
    from psycopg2.extensions import connection as psycopg_connection
    import psycopg2
    
class AuroraPgClient(IAuroraPgClient):
    def __init__(
        self,
        db_host: str,
        db_port: int,
        db_user: str,
        db_password: str,
        db_name: str,
        connection_name: Optional[str] = None,
        sslmode: Literal['require', 'disable'] = "require",
        ssl_root_cert: Optional[str] = None
    ) -> None:
        """
        Initializes the AuroraPgClient with database connection parameters.
        
        Args:
            db_host (str): Aurora PostgreSQL cluster endpoint.
            db_port (int): Port number (default: 5432).
            db_user (str): Database username.
            db_password (str): Database password.
            connection_name (str): glue connection name
            db_name (str): Name of the database to connect to.
            sslmode (Literal['require', 'disable'], optional): SSL mode. Defaults to "require".
            ssl_root_cert (str, optional): Path to SSL root certificate. Defaults to "".
        """
        import psycopg2
        self._db_host: str = db_host
        self._db_port: int = db_port
        self._db_user: str = db_user
        self._db_pass: str = db_password
        self._db_name: str = db_name
        self._connection_name: Optional[str] = connection_name
        self._sslmode: Literal['require', 'disable'] = sslmode
        self._ssl_root_cert: Optional[str] = ssl_root_cert
        self._connection: Optional["psycopg_connection"] = None

    @property
    def db_host(self) -> str:
        return self._db_host

    @property
    def db_port(self) -> int:
        return self._db_port

    @property
    def db_name(self) -> str:
        return self._db_name

    @property
    def connection_name(self) -> Optional[str]:
        return self._connection_name

    @property
    def sslmode(self) -> Literal['require', 'disable']:
        return self._sslmode

    @property
    def ssl_root_cert(self) -> Optional[str]:
        return self._ssl_root_cert

    @property
    def connection(self) -> Optional["psycopg_connection"]:
        return self._connection

    
    def create_client(self) -> "psycopg_connection":
        """
        Establishes a connection to the AWS Aurora PostgreSQL database.

        Returns:
            Optional[PsycopgConnection]: A connection object if successful, else None.
        """
        if self._connection is None or self._connection.closed != 0:
            self._connection = self.create_connection()
        if not self.connection:
            logger.error("Cant return PsycopgConnection")
            raise Exception
        return self.connection

    def create_connection(self) -> Optional["psycopg_connection"]:
        """
        Establishes a connection to the AWS Aurora PostgreSQL database.

        Returns:
            Optional[PsycopgConnection]: A connection object if successful, else None.
        """
        import psycopg2
        try:
           
            self._connection = psycopg2.connect(
                host=self._db_host,
                port=self._db_port,
                user=self._db_user,
                password=self._db_pass,
                dbname=self._db_name,
                sslmode=self._sslmode,
                sslrootcert=self.ssl_root_cert if self.sslmode == 'require' else None
            )
            logger.debug("Connection established successfully.")
            self.fetch_version()
            return self.connection
        except Exception as e:
            logger.error(f"Error connecting to Aurora PostgreSQL: {e}")
            return None

    def fetch_version(self) -> None:
        """
        Fetches and logs the PostgreSQL database version.
        """
        import psycopg2
        if self._connection and self._connection.closed == 0:
            try:
                with self._connection.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    result: tuple | None = cursor.fetchone()
                    if not result :
                        raise Exception
                    logger.debug(f"Database version: {result[0]}")
            except psycopg2.Error as e:
                logger.error(f"Error executing query: {e}")
        else:
            logger.error("Cannot fetch version. Connection is not established.")

    def close_connection(self) -> None:
        """
        Closes the database connection if it exists and is open.
        """
        if self._connection and self._connection.closed == 0:
            self._connection.close()
            logger.debug("Database connection closed.")

    def __enter__(self) -> 'AuroraPgClient':
        """
        Enters the runtime context related to this object.

        Returns:
            AuroraPgClient: The client instance with an active connection.
        """
        self.create_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exits the runtime context and closes the connection.

        Args:
            exc_type: Exception type.
            exc_val: Exception value.
            exc_tb: Traceback.
        """
        self.close_connection()
        if exc_type:
            logger.error(f"An exception occurred: {exc_val}")

    def __del__(self):
        """
        Ensures that the connection is closed upon deletion of the instance.
        """
        self.close_connection()
