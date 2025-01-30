from typing import Optional, Tuple, TYPE_CHECKING
from ...core.interfaces.i_client import IClient
from ...core.logging.logger import logger

if TYPE_CHECKING:
    from opensearchpy import OpenSearch

class OpenSearchClient(IClient):
    
    __slots__: Tuple = (
        "_opensearch_host",
        "_opensearch_user",
        "_opensearch_pass",
        "_opensearch_port",
        "timeout",
        "max_retries",
        "retry_on_timeout",
        "_client",   
    )
    
    def __init__(
        self,
        opensearch_host: str,
        opensearch_user: str,
        opensearch_pass: str,
        opensearch_port: int = 443,
        use_ssl: bool = True,
        verify_certs: bool = False,
        timeout: int = 60,
        max_retries: int = 0,
        retry_on_timeout: bool = True
    ) -> None:
        
        self.opensearch_host: str = opensearch_host
        self.opensearch_user: str = opensearch_user
        self.opensearch_pass: str = opensearch_pass
        self.opensearch_port: int = opensearch_port
        self.use_ssl: bool = use_ssl
        self.verify_certs: bool = verify_certs
        self.timeout: int = timeout
        self.max_retries: int = max_retries
        self.retry_on_timeout: bool = retry_on_timeout
        self._client: Optional["OpenSearch"] = None

    @property
    def client(self) -> "OpenSearch":
        self.create_client()
        if not self._client:
            raise Exception("cant return opensearch client")
        return self._client
        
    def create_client(self) -> "OpenSearch":
        try:
            from opensearchpy import RequestsHttpConnection
            logger.debug("Initializing new OpenSearch client.")

            self._client = OpenSearch(
                hosts=[{'host': self.opensearch_host, 'port': self.opensearch_port}],
                http_auth=(self.opensearch_user, self.opensearch_pass),
                use_ssl=self.use_ssl,
                verify_certs=self.verify_certs,
                connection_class=RequestsHttpConnection,
                timeout=self.timeout,
                max_retries=self.max_retries,
                retry_on_timeout=self.retry_on_timeout
            )
            
            if not self._client.ping():
                logger.error("cant connect to opensearch")
                raise ConnectionError("Cant to connect to OpenSearch")

            logger.info("Successfully connected to OpenSearch")
            return self._client

        except Exception as e:
            logger.error(f"Failed to create OpenSearch client: {e}")
            raise
