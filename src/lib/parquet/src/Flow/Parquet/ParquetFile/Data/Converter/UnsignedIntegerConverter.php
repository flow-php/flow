<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Integer;

use function get_debug_type;
use function is_int;
use function sprintf;

final readonly class UnsignedIntegerConverter implements Converter
{
    public function __construct(
        private string $path,
        private int $bitWidth,
    ) {}

    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        $integer = Integer::forColumn($column);

        return $integer !== null
        && !$integer->isSigned()
        && ($integer->bitWidth() === 32 || $integer->bitWidth() === 64)
            ? new self($column->flatPath(), $integer->bitWidth())
            : null;
    }

    public function fromParquetType(mixed $data): int
    {
        if (!is_int($data)) {
            throw new InvalidArgumentException(sprintf('Expected int, got %s', get_debug_type($data)));
        }

        if ($data >= 0) {
            return $data;
        }

        if ($this->bitWidth === 32) {
            return $data + 4_294_967_296;
        }

        throw new RuntimeException(sprintf('Parquet column "%s" holds a UINT_64 value above PHP_INT_MAX', $this->path));
    }

    public function toParquetType(mixed $data): mixed
    {
        return $data;
    }
}
