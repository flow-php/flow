<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;

final class Codec
{
    public function __construct(
        private readonly Options $options
    ) {
    }

    public function compress(string $data, Compressions $compression) : string
    {
        /**
         * @var false|string $result
         *
         * @psalm-suppress PossiblyInvalidArgument
         */
        $result = match ($compression) {
            Compressions::UNCOMPRESSED => $data,
            Compressions::SNAPPY => \snappy_compress($data),
            /** @phpstan-ignore-next-line */
            Compressions::GZIP => \gzencode($data, $this->options->get(Option::GZIP_COMPRESSION_LEVEL)),
            default => throw new RuntimeException('Compression ' . $compression->name . ' is not supported yet')
        };

        if ($result === false) {
            throw new RuntimeException('Failed to decompress data');
        }

        return $result;
    }

    public function decompress(string $data, Compressions $compression) : string
    {
        /** @var false|string $result */
        $result = match ($compression) {
            Compressions::UNCOMPRESSED => $data,
            Compressions::SNAPPY => \snappy_uncompress($data),
            Compressions::GZIP => \gzdecode($data),
            default => throw new RuntimeException('Compression ' . $compression->name . ' is not supported yet')
        };

        if ($result === false) {
            throw new RuntimeException('Failed to decompress data');
        }

        return $result;
    }
}
