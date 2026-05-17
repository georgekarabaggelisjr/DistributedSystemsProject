import aioboto3
from botocore.exceptions import ClientError
from typing import AsyncGenerator
import logging

logger = logging.getLogger(__name__)


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

    def _get_client(self):
        """
        Βοηθητική μέθοδος που δημιουργεί το async S3 client με τα σωστά credentials.
        Ελέγχει αυτόματα αν το endpoint χρησιμοποιεί http ή https.
        """
        return self.session.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=self.endpoint.startswith('https')
        )

    async def upload_file(self, bucket_name: str, file_path: str, file_obj) -> str:
        """
        Uploads data to MinIO using streaming (upload_fileobj).
        'file_obj' should be a file-like object (e.g., UploadFile.file).
        """
        async with self._get_client() as client:
            try:
                await client.head_bucket(Bucket=bucket_name)
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logger.info(f"Bucket '{bucket_name}' not found. Creating it...")
                    await client.create_bucket(Bucket=bucket_name)
                else:
                    raise e

            # Χρήση της upload_fileobj για streaming
            # Αυτή η μέθοδος διαχειρίζεται αυτόματα τα chunks
            await client.upload_fileobj(file_obj, bucket_name, file_path)

            return f"s3://{bucket_name}/{file_path}"


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
        async with self._get_client() as client:
            try:
                # Ask for the object-file
                response = await client.get_object(Bucket=bucket_name, Key=file_path)

                # Read the response as chunks of 1 Megabyte (1024 * 1024 bytes)
                async for chunk in response['Body'].iter_chunks(chunk_size=1048576):
                    yield chunk
            except ClientError as e:
                logger.error(f"Error streaming file {file_path} from {bucket_name}: {e}")
                raise e

    async def check_connection(self) -> bool:
        """
        Health check method to ensure MinIO is reachable during UI Service startup.
        Performs a simple list_buckets(), to check if MinIO answers.
        """
        try:
            async with self._get_client() as client:
                await client.list_buckets()
                return True
        except Exception as e:
            logger.error(f"MinIO health check failed. Could not connect to {self.endpoint}: {e}")
            return False