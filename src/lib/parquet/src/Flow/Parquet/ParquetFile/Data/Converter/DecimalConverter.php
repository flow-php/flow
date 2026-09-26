<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function Flow\Parquet\Binary\decimal_from_unscaled;
use function Flow\Parquet\Binary\decimal_unscaled;

final readonly class DecimalConverter implements Converter
{
    public function __construct(
        private int $precision,
        private int $scale,
    ) {}

    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        $decimal = $column->logicalType()?->decimalData();

        return ($column->type() === PhysicalType::INT32 || $column->type() === PhysicalType::INT64) && $decimal !== null
            ? new self($decimal->precision(), $decimal->scale())
            : null;
    }

    public function fromParquetType(mixed $data): float
    {
        /** @var int $data */
        return decimal_from_unscaled((string) $data, $this->scale);
    }

    public function toParquetType(mixed $data): int
    {
        /** @var float|int $data */
        return (int) decimal_unscaled((float) $data, $this->precision, $this->scale);
    }
}
