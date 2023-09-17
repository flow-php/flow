<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\RuntimeException;

if (!\function_exists('snappy_uncompress')) {
    /** @psalm-suppress LessSpecificReturnType */
    function snappy_uncompress(string $data) : string|false
    {
        throw new \Flow\ETL\Exception\RuntimeException('snappy_uncompress() is not available. Please install php-snappy extension https://github.com/kjdev/php-ext-snappy');
    }
}

final class Codec
{
    public function decompress(string $data, Compressions $compression) : string
    {
        /** @var false|string $result */
        $result = match ($compression) {
            Compressions::UNCOMPRESSED => $data,
            Compressions::SNAPPY => snappy_uncompress($data),
            Compressions::GZIP => \gzdecode($data),
            default => throw new RuntimeException('Compression ' . $compression->name . ' is not supported yet')
        };

        if ($result === false) {
            throw new RuntimeException('Failed to decompress data');
        }

        return $result;
    }
}
