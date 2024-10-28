import os
import gzip
import bz2
import lzma
import zlib
import zstandard as zstd
from typing import Union, BinaryIO, Dict, Callable
from abc import ABC, abstractmethod

class CompressionStrategy(ABC):
    @abstractmethod
    def compress(self, data: bytes, compression_level: int) -> bytes:
        pass

    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        pass

class ZstdStrategy(CompressionStrategy):
    def compress(self, data: bytes, compression_level: int) -> bytes:
        compressor = zstd.ZstdCompressor(level=compression_level)
        return compressor.compress(data)

    def decompress(self, data: bytes) -> bytes:
        decompressor = zstd.ZstdDecompressor()
        return decompressor.decompress(data)

class GzipStrategy(CompressionStrategy):
    def compress(self, data: bytes, compression_level: int) -> bytes:
        return gzip.compress(data, compresslevel=compression_level)

    def decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)

class Bz2Strategy(CompressionStrategy):
    def compress(self, data: bytes, compression_level: int) -> bytes:
        return bz2.compress(data, compresslevel=compression_level)

    def decompress(self, data: bytes) -> bytes:
        return bz2.decompress(data)

class LzmaStrategy(CompressionStrategy):
    def compress(self, data: bytes, compression_level: int) -> bytes:
        return lzma.compress(data, preset=compression_level)

    def decompress(self, data: bytes) -> bytes:
        return lzma.decompress(data)

class ZlibStrategy(CompressionStrategy):
    def compress(self, data: bytes, compression_level: int) -> bytes:
        return zlib.compress(data, level=compression_level)

    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)


class FileCompressor:
    def __init__(self, compression_level: int = 6):
        """Initialize FileCompressor with compression level.

        Args:
            compression_level: Integer between 1-9, where 9 is highest compression.

        Raises:
            ValueError: If compression level is not between 1 and 9.
        """
        if not 1 <= compression_level <= 9:
            raise ValueError("Compression level must be between 1 and 9")
        self.compression_level = compression_level
        self.strategies: Dict[str, CompressionStrategy] = {
            'gz': GzipStrategy(),
            'bz2': Bz2Strategy(),
            'xz': LzmaStrategy(),
            'lzma': LzmaStrategy(),
            'zlib': ZlibStrategy(),
            'zstd': ZstdStrategy()
        }

    def _read_input(self, input_file: Union[str, BinaryIO]) -> bytes:
        if isinstance(input_file, str):
            with open(input_file, 'rb') as f:
                return f.read()
        return input_file.read()

    def _write_output(self, output_file: str, data: bytes) -> None:
        with open(output_file, 'wb') as f:
            f.write(data)

    def compress(self, input_file: Union[str, BinaryIO], output_file: str, format: str = 'zstd') -> None:
        """Compress the input file to the specified format.

        Args:
            input_file: Path to input file or file-like object.
            output_file: Path for compressed output file.
            format: Compression format ('gz', 'bz2', 'xz', 'lzma', 'zlib').

        Raises:
            ValueError: If format is not supported.
        """
        if format not in self.strategies:
            raise ValueError(f"Unsupported format. Choose from: {', '.join(self.strategies.keys())}")

        data = self._read_input(input_file)
        compressed_data = self.strategies[format].compress(data, self.compression_level)
        self._write_output(output_file, compressed_data)

    def decompress(self, input_file: Union[str, BinaryIO], output_file: str, format: str = 'zstd') -> None:
        """Decompress the input file from the specified format.

        Args:
            input_file: Path to compressed input file or file-like object.
            output_file: Path for decompressed output file.
            format: Compression format ('gz', 'bz2', 'xz', 'lzma', 'zlib').

        Raises:
            ValueError: If format is not supported.
        """
        if format not in self.strategies:
            raise ValueError(f"Unsupported format. Choose from: {', '.join(self.strategies.keys())}")

        compressed_data = self._read_input(input_file)
        decompressed_data = self.strategies[format].decompress(compressed_data)
        self._write_output(output_file, decompressed_data)

    def get_compression_ratio(self, original_file: str, compressed_file: str) -> float:
        """Calculate the compression ratio.

        Args:
            original_file: Path to original file.
            compressed_file: Path to compressed file.

        Returns:
            float: Compression ratio (original size / compressed size).
        """
        original_size = os.path.getsize(original_file)
        compressed_size = os.path.getsize(compressed_file)
        return original_size / compressed_size


if __name__ == "__main__":
    compressor = FileCompressor(compression_level=6)

    with open("example.txt", "w") as f:
        f.write("This is some example text to compress.")

    compressor.compress("example.txt", "example.txt.gz", format="gz")
    compressor.decompress("example.txt.gz", "example_gz_decompressed.txt", format="gz")
    gz_ratio = compressor.get_compression_ratio("example.txt", "example.txt.gz")
    print(f"GZip compression ratio: {gz_ratio:.2f}")
