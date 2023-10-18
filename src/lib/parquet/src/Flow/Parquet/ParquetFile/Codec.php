<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\RuntimeException;

final class Codec
{
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
