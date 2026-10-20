<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Integer;

use function get_debug_type;
use function is_int;
use function sprintf;

final readonly class UnsignedInt32Converter implements Converter
{
    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        return Integer::forColumn($column)?->is(32, false) === true ? new self() : null;
    }

    public function fromParquetType(mixed $data): int
    {
        if (!is_int($data)) {
            throw new InvalidArgumentException(sprintf('Expected int, got %s', get_debug_type($data)));
        }

        return $data >= 0 ? $data : $data + 4_294_967_296;
    }

    public function toParquetType(mixed $data): mixed
    {
        return $data;
    }
}
