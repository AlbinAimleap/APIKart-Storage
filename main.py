from typing import Union, Optional
from pathlib import Path
from compressor import FileCompressor
from digital_ocean import DigitalOceanStorage
import aiofiles
from icecream import ic
from schema import FileSchema
import asyncio
import time
from pydb.logger import setup_logger
from utils import ExceptionHandler

exception_handler = ExceptionHandler()

logger = setup_logger("Async Object Storage")

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
    
    async def list_objects(self, prefix: str = "") -> list:
        """List all objects in the storage bucket.
        
        Args:
            prefix (str): Filter objects by prefix/folder path
            
        Returns:
            list: List of object names in the bucket
        """
        try:
            if prefix and not prefix.endswith('/'):
                prefix += '/'
            return await self.storage.list_objects(prefix)
        except Exception as e:
            print(f"Error listing objects: {str(e)}")
            return []
    
    @exception_handler.try_except(verbose=True)
    async def compress_and_upload(
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
            async with aiofiles.tempfile.NamedTemporaryFile(suffix=f'.{format}', delete=False) as temp_file:
                compressed_path = temp_file.name
            
            compressed_size = await self.compressor.compress(str(input_file), compressed_path, format)
            original_size = Path(input_file).stat().st_size
            
            
            object_name_with_ext = f"{object_name}.{format}"
            if folder_path:
                object_name_with_ext = f"{folder_path.rstrip('/')}/{object_name_with_ext}"
            
            # logger.info(f"Uploading {input_file.name}")
            
            url = await self.storage.store_object(compressed_path, object_name_with_ext, public_access)
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

    async def download_and_decompress(
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
                
            async with aiofiles.tempfile.NamedTemporaryFile(suffix=f'.{format}', delete=False) as temp_file:
                temp_compressed = temp_file.name
            
            success = await self.storage.download_file(object_path, temp_compressed)
            if not success:
                if Path(temp_compressed).exists():
                    Path(temp_compressed).unlink()
                return False
            
            await self.compressor.decompress(temp_compressed, str(output_file), format)
            
            if Path(temp_compressed).exists():
                Path(temp_compressed).unlink()
            
            return True
        except Exception as e:
            print(f"Error in download and decompress: {str(e)}")
            if Path(temp_compressed).exists():
                Path(temp_compressed).unlink()
            return False

async def main():
    main_process = time.time()
    storage = ObjectStorage(bucket_name='aimleap-storage')
    cur_dir = Path(__file__).resolve().parent
    input_path = cur_dir / "page_data_new"

    files = list(input_path.iterdir())[:100]
    files_count = len(files)
    ic(files_count)
    
    start_time = time.time()
    tasks = [storage.compress_and_upload(file, file.name, folder_path='data/html', public_access=False) for file in files if file.is_file()]
    await asyncio.gather(*tasks)
    elapsed_time = time.time() - start_time
    compress_and_upload = f"Compression and upload time: {elapsed_time:.2f} seconds"
    ic(compress_and_upload)

    start_time = time.time()
    objects = await storage.list_objects('data/html')
    # ic(objects)
    elapsed_time = time.time() - start_time
    list_objects = f"List objects time: {elapsed_time:.2f} seconds"
    ic(list_objects)

    start_time = time.time()
    downloaded = await storage.download_and_decompress(
        '24.95Z OREO PARTY ORIG DBL STUF 8 - Walmart.com.html.zstd',
        cur_dir / '24.95Z OREO PARTY ORIG DBL STUF 8 - Walmart.com.html',
        folder_path='data/html'
    )
    ic(downloaded)
    elapsed_time = time.time() - start_time
    download_and_decompress = f"Download and decompress time: {elapsed_time:.2f} seconds"
    ic(download_and_decompress)
    main_process_end = time.time() - main_process
    total_time = f"Total time to process 100 files, list objects, download and decompress 1 file: {main_process_end:.2f} seconds"
    ic(total_time)
    
if __name__ == "__main__":
    asyncio.run(main())
