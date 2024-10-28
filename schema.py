from pydb.core import BaseSchema
from pydantic import Field
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, timezone


class FileSchema(BaseSchema):
    file_name: str = Field(..., description="File name")
    file_name_original: str = Field(..., description="Original file name")
    file_type: str = Field(..., description="File type")
    file_url: str = Field(..., description="File URL")
    file_compression_type: str = Field(..., description="File Compression type")
    created_at: datetime = Field(description="Created at", default=datetime.now(tz=timezone.utc))

