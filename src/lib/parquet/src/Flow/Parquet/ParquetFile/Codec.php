<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\{Option, Options};

final readonly class Codec
{
    public function __construct(
        private Options $options,
    ) {
    }

    public function compress(string $data, Compressions $compression) : string
    {
        /**
         * @var false|string $result
         */
        $result = match ($compression) {
            Compressions::UNCOMPRESSED => $data,
            Compressions::SNAPPY => \snappy_compress($data),
            Compressions::BROTLI => \brotli_compress($data, $this->options->getInt(Option::BROTLI_COMPRESSION_LEVEL)),
            Compressions::GZIP => \gzencode($data, $this->options->getInt(Option::GZIP_COMPRESSION_LEVEL)),
            Compressions::LZ4 => \lz4_compress($data, $this->options->getInt(Option::LZ4_COMPRESSION_LEVEL)),
            Compressions::LZ4_RAW => \lz4_compress($data, $this->options->getInt(Option::LZ4_COMPRESSION_LEVEL)),
            Compressions::ZSTD => \zstd_compress($data, $this->options->getInt(Option::ZSTD_COMPRESSION_LEVEL)),
            default => throw new RuntimeException('Compression ' . $compression->name . ' is not supported yet'),
        };

        if ($result === false) {
            throw new RuntimeException('Failed to compress data');
        }

        return $result;
    }

    public function decompress(string $data, Compressions $compression) : string
    {
        /** @var false|string $result */
        $result = match ($compression) {
            Compressions::UNCOMPRESSED => $data,
            Compressions::SNAPPY => \snappy_uncompress($data),
            Compressions::BROTLI => \brotli_uncompress($data),
            Compressions::GZIP => \gzdecode($data),
            Compressions::LZ4 => \lz4_uncompress($data),
            Compressions::LZ4_RAW => \lz4_uncompress($data),
            Compressions::ZSTD => \zstd_uncompress($data),
            default => throw new RuntimeException('Compression ' . $compression->name . ' is not supported yet'),
        };

        if ($result === false) {
            throw new RuntimeException('Failed to decompress data');
        }

        return $result;
    }
}
