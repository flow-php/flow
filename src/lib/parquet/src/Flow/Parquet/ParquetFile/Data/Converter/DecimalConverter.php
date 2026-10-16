<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function Flow\Parquet\Binary\decimal_from_unscaled;
use function Flow\Parquet\Binary\decimal_unscaled;
use function Flow\Parquet\Binary\decode_decimal;
use function Flow\Parquet\Binary\encode_decimal;
use function get_debug_type;
use function is_float;
use function is_int;

final readonly class DecimalConverter implements Converter
{
    public function __construct(
        private PhysicalType $physicalType,
        private int $precision,
        private int $scale,
        private ?int $byteLength = null,
    ) {}

    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        $decimal = $column->logicalType()?->decimalData();

        return $decimal !== null && $column->type() !== PhysicalType::INT96
            ? new self(
                $column->type(),
                $decimal->precision(),
                $decimal->scale(),
                $column->type() === PhysicalType::FIXED_LEN_BYTE_ARRAY ? $column->typeLength() : null,
            )
            : null;
    }

    public function fromParquetType(mixed $data): float
    {
        /** @var int|string $data */
        return is_int($data)
            ? decimal_from_unscaled((string) $data, $this->scale)
            : decode_decimal($data, $this->scale);
    }

    public function toParquetType(mixed $data): int|string
    {
        if (!is_int($data) && !is_float($data)) {
            throw new InvalidArgumentException('Decimal must be written as int or float, got ' . get_debug_type($data));
        }

        return match ($this->physicalType) {
            PhysicalType::INT32, PhysicalType::INT64 => (int) decimal_unscaled(
                (float) $data,
                $this->precision,
                $this->scale,
            ),
            default => encode_decimal((float) $data, $this->precision, $this->scale, $this->byteLength),
        };
    }
}
