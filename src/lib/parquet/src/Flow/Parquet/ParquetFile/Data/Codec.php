<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;

use function brotli_compress;
use function brotli_uncompress;
use function gzdecode;
use function gzencode;
use function lz4_compress;
use function lz4_uncompress;
use function pack;
use function snappy_compress;
use function snappy_uncompress;
use function strlen;
use function substr;
use function zstd_compress;
use function zstd_uncompress;

final readonly class Codec
{
    public function __construct(
        private Options $options,
    ) {}

    public function compress(string $data, Compressions $compression): string
    {
        $result = match ($compression) {
            Compressions::UNCOMPRESSED => $data,
            Compressions::SNAPPY => snappy_compress($data),
            Compressions::BROTLI => brotli_compress($data, $this->options->getInt(Option::BROTLI_COMPRESSION_LEVEL)),
            Compressions::GZIP => gzencode($data, $this->options->getInt(Option::GZIP_COMPRESSION_LEVEL)),
            Compressions::LZ4 => lz4_compress($data, $this->options->getInt(Option::LZ4_COMPRESSION_LEVEL)),
            // @mago-ignore analysis:redundant-cast
            Compressions::LZ4_RAW => substr(
                (string) lz4_compress($data, $this->options->getInt(Option::LZ4_COMPRESSION_LEVEL)),
                4,
            ),
            Compressions::ZSTD => zstd_compress($data, $this->options->getInt(Option::ZSTD_COMPRESSION_LEVEL)),
            default => throw new RuntimeException('Compression ' . $compression->name . ' is not supported yet'),
        };

        if ($result === false) {
            throw new RuntimeException('Failed to compress data');
        }

        return $result;
    }

    public function decompress(string $data, Compressions $compression, ?int $uncompressedSize = null): string
    {
        $result = match ($compression) {
            Compressions::UNCOMPRESSED => $data,
            Compressions::SNAPPY => snappy_uncompress($data),
            Compressions::BROTLI => brotli_uncompress($data),
            Compressions::GZIP => gzdecode($data),
            Compressions::LZ4 => @lz4_uncompress($data),
            Compressions::LZ4_RAW => @lz4_uncompress(pack('V', $uncompressedSize ?? strlen($data)) . $data),
            Compressions::ZSTD => zstd_uncompress($data),
            default => throw new RuntimeException('Compression ' . $compression->name . ' is not supported yet'),
        };

        if ($result === false) {
            throw new RuntimeException('Failed to decompress data');
        }

        return $result;
    }
}
