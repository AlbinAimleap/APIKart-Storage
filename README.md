  # Object Storage with Compression

  A Python library that combines file compression and Digital Ocean Spaces storage functionality.

  ## Features

  - File compression using multiple formats (zstd, gz, bz2, xz, lzma, zlib)
  - Upload compressed files to Digital Ocean Spaces
  - Download and decompress files from storage
  - List objects in storage buckets
  - Optional public access settings
  - File metadata tracking
  - Exception handling and logging
  - Async/await support for all operations

  ## Installation

```bash
  pip install -r requirements.txt
```

  ## Usage

```python
  from digital_ocean import ObjectStorage
  import asyncio
```

  # Initialize with bucket name and compression settings
```python
  storage = ObjectStorage(
      bucket_name='your-bucket-name',
      compression_level=6,
      public_access=False
  )
```

  # Compress and upload a file
```python
  url = await storage.compress_and_upload(
      input_file='path/to/file.txt',
      object_name='file.txt',
      folder_path='data/files',
      format='zstd',
      public_access=False
  )
  ```


  # List objects in storage
```python
  objects = await storage.list_objects(prefix='data/files')
```

  # Download and decompress a file
```python
  success = await storage.download_and_decompress(
      object_name='file.txt.zstd',
      output_file='path/to/output.txt',
      folder_path='data/files',
      format='zstd'
  )
```

  ## Configuration

  The following environment variables are required:

  - Digital Ocean
      - `DO_SPACES_ENDPOINT`: Digital Ocean Space endpoint
      - `DO_SPACES_REGION`: Digital Ocean Space region
      - `DO_SPACES_KEY`: Digital Ocean access key
      - `DO_SPACES_SECRET`: Digital Ocean secret key

  - Database Configs (Optional:- defaults to sqlite)
      - `DB_ENGINE`: Database type (e.g., sqlite, mysql, postgresql)
      - `DB_HOST`: Database host
      - `DB_PORT`: Database port
      - `DB_NAME`: Database name
      - `DB_USER`: Database user
      - `DB_PASSWORD`: Database password

  ## Performance

  - Fast compression using zstandard (zstd) format
  - Asynchronous operations for improved performance
  - Efficient bulk upload/download operations using asyncio
  - Temporary file handling for memory efficiency
  - Exception handling with detailed logging
