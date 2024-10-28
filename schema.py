from pydb.core import BaseSchema
from pydantic import Field
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, timezone


class FileSchema(BaseSchema):
    file_name: str = Field(..., description="Original file name")
    file_type: str = Field(..., description="File type")
    original_file_size: float = Field(..., description="Original file size")
    file_compression_type: str = Field(..., description="File Compression type")
    key: str = Field(..., description="File name")
    file_url: str = Field(..., description="File URL")
    compressed_file_size: float = Field(..., description="Compressed file size")
    created_at: datetime = Field(description="Created at", default=datetime.now(tz=timezone.utc))

