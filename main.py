from typing import Union, Optional
from pathlib import Path
from compressor import FileCompressor
from digital_ocean import DigitalOceanStorage
import tempfile
from icecream import ic
from schema import FileSchema

class ObjectStorage:
    """A class that combines compression and storage functionality."""
    
    def __init__(self, bucket_name: str, compression_level: int = 6, public_access: bool = False):
        """Initialize ObjectStorage with storage and compression settings.
        
        Args:
            bucket_name (str): Name of the DigitalOcean Spaces bucket
            compression_level (int): Compression level (1-9). Defaults to 6
            public_access (bool): Set default public access for uploads. Defaults to False
        """
        self.storage = DigitalOceanStorage(bucket_name, public_access)
        self.compressor = FileCompressor(compression_level)
    
    def list_objects(self, prefix: str = "") -> list:
        """List all objects in the storage bucket.
        
        Args:
            prefix (str): Filter objects by prefix/folder path
            
        Returns:
            list: List of object names in the bucket
        """
        try:
            # Ensure prefix ends with forward slash for directory listing
            if prefix and not prefix.endswith('/'):
                prefix += '/'
                
            objects = self.storage.list_objects(prefix)
            return objects
        except Exception as e:
            print(f"Error listing objects: {str(e)}")
            return []    
    
    def compress_and_upload(
        self, 
        input_file: Union[str, Path], 
        object_name: str,
        folder_path: str = "",
        format: str = 'zstd',
        public_access: Optional[bool] = None
        ) -> Optional[str]:
        """Compress a file and upload it to DigitalOcean Spaces.
        
        Args:
            input_file (Union[str, Path]): Path to input file
            object_name (str): Name for the object in storage
            folder_path (str): Path to folder in storage (e.g. "folder1/subfolder")
            format (str): Compression format ('gz', 'bz2', 'xz', 'lzma', 'zlib')
            public_access (Optional[bool]): Override default public access setting
            
        Returns:
            Optional[str]: URL of the uploaded compressed object if successful, None if failed
        """
        try:
            with tempfile.NamedTemporaryFile(suffix=f'.{format}', delete=False) as temp_file:
                compressed_path = temp_file.name
                self.compressor.compress(str(input_file), compressed_path, format)
            
            object_name_with_ext = f"{object_name}.{format}"
            if folder_path:
                object_name_with_ext = f"{folder_path.rstrip('/')}/{object_name_with_ext}"
            
            url = self.storage.store_object(compressed_path, object_name_with_ext, public_access)
            Path(compressed_path).unlink()
            
            schema = FileSchema(
                file_name=str(input_file.name),
                file_type=Path(input_file).suffix.replace('.', ''),
                original_file_size=original_size,
                file_compression_type=format,
                key=object_name_with_ext,
                file_url=url,
                compressed_file_size=compressed_size
            )
            schema.save()
            return url
        except Exception as e:
            print(f"Error in compress and upload: {str(e)}")
            if Path(compressed_path).exists():
                Path(compressed_path).unlink()
            return None 
        
    def download_and_decompress(
        self,
        object_name: str,
        output_file: Union[str, Path],
        folder_path: str = "",
        format: str = 'zstd'
        ) -> bool:
        """Download and decompress a file from DigitalOcean Spaces.
        
        Args:
            object_name (str): Name of the object in storage
            output_file (Union[str, Path]): Path for decompressed output file
            folder_path (str): Path to folder in storage (e.g. "folder1/subfolder")
            format (str): Compression format ('gz', 'bz2', 'xz', 'lzma', 'zlib')
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if folder_path:
                object_path = f"{folder_path.rstrip('/')}/{object_name}"
            else:
                object_path = object_name
                
            with tempfile.NamedTemporaryFile(suffix=f'.{format}', delete=False) as temp_file:
                temp_compressed = temp_file.name
            
            success = self.storage.download_file(object_path, temp_compressed)
            
            if not success:
                if Path(temp_compressed).exists():
                    Path(temp_compressed).unlink()
                return False
            
            self.compressor.decompress(temp_compressed, str(output_file), format)
            
            if Path(temp_compressed).exists():
                Path(temp_compressed).unlink()
            
            return True
        except Exception as e:
            print(f"Error in download and decompress: {str(e)}")
            if Path(temp_compressed).exists():
                Path(temp_compressed).unlink()
            return False

def main():
    """
    Runs the main logic of the application, including compressing and uploading files to DigitalOcean Spaces, listing the uploaded objects, and downloading and decompressing a specific file.
    Args:
        None
    Returns:
        None
    """
    storage = ObjectStorage(bucket_name='aimleap-storage')
    cur_dir = Path(__file__).resolve().parent
    input_path = cur_dir / "page_data_new"
    
    for file in list(input_path.iterdir())[:10]:
        if file.is_file():
            url = storage.compress_and_upload(file, file.name, folder_path='data/html')
            ic(url)
    
    # List objects in the specified folder
    objects = storage.list_objects('data/html')
    ic(objects)
    
    # Download and decompress a specific file
    downloaded = storage.download_and_decompress('24.95Z OREO PARTY ORIG DBL STUF 8 - Walmart.com.html.zstd', cur_dir / '24.95Z OREO PARTY ORIG DBL STUF 8 - Walmart.com.html', folder_path='data/html')
    ic(downloaded)
      
      
if __name__ == "__main__":
    main()