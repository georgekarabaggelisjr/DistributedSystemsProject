import aioboto3
from botocore.exceptions import ClientError


class StorageClient:
    """
    Asynchronous wrapper for MinIO (S3-compatible) operations using aioboto3.
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str):
        """
        Initializes the async session.
        Note: For local MinIO/Kubernetes, 'secure' is usually False (http).
        """
        self.session = aioboto3.Session()
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key

    async def upload_file(self, bucket_name: str, file_path: str, file_stream: bytes) -> str:
        """
        Uploads data to MinIO without blocking the FastAPI event loop.

        1. Starts an async client session.
        2. Checks if bucket exists (optional/admin task).
        3. Performs 'put_object' using the binary stream.

        Returns:
            str: The internal S3 URI
        """
        pass

    async def get_file_stream(self, bucket_name: str, file_path: str):
        """
        Retrieves a file as an AsyncGenerator to stream it back to the CLI.

        Workflow:
        1. Opens an async connection.
        2. Requests the object.
        3. Yields chunks of bytes

        Returns:
            AsyncGenerator: Bytes for the StreamingResponse.
        """
        pass

    async def check_connection(self) -> bool:
        """
        Health check method to ensure MinIO is reachable during UI Service startup.
        """
        pass