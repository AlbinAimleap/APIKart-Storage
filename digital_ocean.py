import asyncio
from typing import Union, Optional, List
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import os
import aioboto3
from botocore.config import Config

load_dotenv(find_dotenv(".env"))

class DigitalOceanStorage:
    """A class to handle interactions with DigitalOcean Spaces storage asynchronously."""

    def __init__(self, bucket_name: str, public_access: bool = False) -> None:
        """Initialize DigitalOcean Storage client."""
        self.bucket_name = bucket_name
        self.session = aioboto3.Session()
        self.public_access = public_access
        self.config = Config(signature_version='s3v4', region_name=os.getenv('DO_SPACES_REGION'))
        self.endpoint_url = os.getenv('DO_SPACES_ENDPOINT')
        self.access_key = os.getenv('DO_SPACES_KEY')
        self.secret_key = os.getenv('DO_SPACES_SECRET')

    async def create_bucket(self, bucket_name: str) -> None:
        """Asynchronously create a new DigitalOcean Spaces bucket."""
        async with self.session.client(
                's3', config=self.config, endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key
        ) as client:
            try:
                await client.head_bucket(Bucket=bucket_name)
            except client.exceptions.NoSuchBucket:
                await client.create_bucket(Bucket=bucket_name)

    async def store_object(self, file_path: Union[Path, str], object_name: str, public_access: Optional[bool] = None) -> Optional[str]:
        """Asynchronously store an object in the DigitalOcean Space bucket."""
        public_access = public_access if public_access is not None else self.public_access
        async with self.session.client(
                's3', config=self.config, endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key
        ) as client:
            try:
                await client.upload_file(
                    str(file_path),
                    self.bucket_name,
                    object_name,
                    ExtraArgs={'ACL': 'public-read'} if public_access else {}
                )
                return f"https://{self.bucket_name}.blr1.digitaloceanspaces.com/{object_name}"
            except Exception as e:
                print(f"Error uploading file: {str(e)}")
                return None

    async def list_objects(self, prefix: str = "") -> List[str]:
        """Asynchronously list all objects in the bucket with optional prefix filter."""
        async with self.session.client(
                's3', config=self.config, endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key
        ) as client:
            try:
                response = await client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
                return [obj['Key'] for obj in response.get('Contents', [])]
            except Exception as e:
                print(f"Error listing objects: {str(e)}")
                return []

    async def get_object_url(self, object_name: str) -> str:
        """Generate the URL for an object in the bucket."""
        return f"https://{self.bucket_name}.blr1.digitaloceanspaces.com/{object_name}"
    
    async def download_file(self, object_name: str, file_path: Union[Path, str]) -> bool:
        """Asynchronously download a file from the bucket."""
        async with self.session.client(
                's3', config=self.config, endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key
        ) as client:
            try:
                await client.download_file(
                    self.bucket_name,
                    object_name,
                    str(file_path)
                )
                return True
            except Exception as e:
                print(f"Error downloading file: {str(e)}")
                return False

async def main():
    storage = DigitalOceanStorage(bucket_name='api-kart-space')
    cur_dir = Path(__file__).resolve().parent
    path = cur_dir / 'test.json'
    name = 'test.json'
    
    res = await storage.store_object(path, name)
    print(res)
    
    # List all objects
    objects = await storage.list_objects()
    print("Objects in bucket:", objects)
    
    object_url = await storage.get_object_url(name)
    print(object_url)

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
