from typing import Union, Optional, List
import boto3
from botocore.client import Config
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv(".env"))

class DigitalOceanStorage:
    """A class to handle interactions with DigitalOcean Spaces storage.

    This class provides methods to store and manage objects in DigitalOcean Spaces,
    which is compatible with the S3 protocol.

    Attributes:
        cur_dir (Path): Current directory path
        client (boto3.client): Boto3 S3 client for DigitalOcean Spaces
        public_access (bool): Default access policy for uploaded objects
    """

    def __init__(self, bucket_name: str, public_access: bool = False) -> None:
        """Initialize DigitalOcean Storage client.

        Args:
            bucket_name (str): Name of the DigitalOcean Spaces bucket.
            public_access (bool, optional): Set default public access for uploads. Defaults to False.
        """
        self.bucket_name = bucket_name
        session = boto3.session.Session()
        self.client = session.client('s3',
            config=Config(signature_version='s3v4'),
            region_name=os.getenv('DO_SPACES_REGION'),
            endpoint_url=os.getenv('DO_SPACES_ENDPOINT'),
            aws_access_key_id=os.getenv('DO_SPACES_KEY'),
            aws_secret_access_key=os.getenv('DO_SPACES_SECRET')
        )            
        self.public_access = public_access
    
    def create_bucket(self, bucket_name: str) -> None:
        """Create a new DigitalOcean Spaces bucket.
        
        Args:
            bucket_name (str): Name of the DigitalOcean Spaces bucket.
        Returns:
            Self: Instance of the DigitalOceanStorage class
        """
        # Create bucket if it doesn't exist
        try:
            self.client.head_bucket(Bucket=bucket_name)
        except:
            self.client.create_bucket(Bucket=bucket_name)
        return self

    def store_object(self, file_path: Union[Path, str], object_name: str, public_access: Optional[bool] = None) -> Optional[str]:
        """Store an object in the DigitalOcean Space bucket.

        Args:
            file_path (Union[Path, str]): Path to the file to upload
            object_name (str): Name to give the object in the bucket
            public_access (Optional[bool], optional): Override default public access setting. Defaults to None.

        Returns:
            Optional[str]: URL of the uploaded object if successful, None if failed
        """
        public_access = public_access if public_access is not None else self.public_access
        try:
            self.client.upload_file(
                str(file_path),
                self.bucket_name,
                object_name,
                ExtraArgs={'ACL': 'public-read'} if public_access else {}
            )
            return f"https://{self.bucket_name}.blr1.digitaloceanspaces.com/{object_name}"
        except Exception as e:
            print(f"Error uploading file: {str(e)}")
            return None

    def list_objects(self, prefix: str = "") -> List[str]:
        """List all objects in the bucket with optional prefix filter.

        Args:
            prefix (str, optional): Filter objects by prefix. Defaults to "".

        Returns:
            List[str]: List of object keys in the bucket
        """
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except Exception as e:
            print(f"Error listing objects: {str(e)}")
            return []

    def get_object_url(self, object_name: str) -> str:
        """Generate the URL for an object in the bucket.

        Args:
            object_name (str): Name of the object

        Returns:
            str: Full URL to the object
        """
        return f"https://{self.bucket_name}.blr1.digitaloceanspaces.com/{object_name}"
    
    def download_file(self, object_name: str, file_path: Union[Path, str]) -> bool:
            """Download a file from the bucket.
    
            Args:
                object_name (str): Name of the object in the bucket
                file_path (Union[Path, str]): Local path where to save the downloaded file
    
            Returns:
                bool: True if download was successful, False otherwise
            """
            try:
                self.client.download_file(
                    self.bucket_name,
                    object_name,
                    str(file_path)
                )
                return True
            except Exception as e:
                print(f"Error downloading file: {str(e)}")
                return False
    

if __name__ == "__main__":
    storage = DigitalOceanStorage(bucket_name='api-kart-space')
    cur_dir = Path(__file__).resolve().parent
    path = cur_dir / 'test.json'
    name = 'test.json'
    res = storage.store_object(path, name)
    print(res)
    
    # List all objects
    objects = storage.list_objects()
    print("Objects in bucket:", objects)
    
    object_ = storage.get_object_url(name)
    print(object_)