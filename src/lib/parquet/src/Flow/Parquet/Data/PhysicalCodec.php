<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function chr;
use function Flow\Parquet\Binary\decode_f32;
use function Flow\Parquet\Binary\decode_f64;
use function Flow\Parquet\Binary\decode_i32;
use function Flow\Parquet\Binary\decode_i64;
use function Flow\Parquet\Binary\encode_f32;
use function Flow\Parquet\Binary\encode_f64;
use function Flow\Parquet\Binary\encode_i32;
use function Flow\Parquet\Binary\encode_i64;
use function get_debug_type;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function ord;
use function sprintf;
use function strlen;

final readonly class PhysicalCodec
{
    /**
     * Null for bytes whose width does not fit the physical type: older flow writers doubled INT64 bounds.
     */
    public function decodeStatistic(FlatColumn $column, string $bytes): bool|float|int|string|null
    {
        $width = $this->statisticWidth($column);

        if ($width !== null && strlen($bytes) !== $width) {
            return null;
        }

        return match ($column->type()) {
            PhysicalType::BOOLEAN => ord($bytes) !== 0,
            PhysicalType::INT32 => decode_i32(ByteOrder::LITTLE_ENDIAN, $bytes)[0],
            PhysicalType::INT64 => decode_i64(ByteOrder::LITTLE_ENDIAN, $bytes)[0],
            PhysicalType::FLOAT => decode_f32(ByteOrder::LITTLE_ENDIAN, $bytes)[0],
            PhysicalType::DOUBLE => decode_f64(ByteOrder::LITTLE_ENDIAN, $bytes)[0],
            PhysicalType::INT96, PhysicalType::FIXED_LEN_BYTE_ARRAY, PhysicalType::BYTE_ARRAY => $bytes,
        };
    }

    public function encodeStatistic(FlatColumn $column, mixed $physical): string
    {
        $encoded = match ($column->type()) {
            PhysicalType::BOOLEAN => is_bool($physical) ? chr((int) $physical) : null,
            PhysicalType::INT32 => is_int($physical) ? encode_i32(ByteOrder::LITTLE_ENDIAN, [$physical]) : null,
            PhysicalType::INT64 => is_int($physical) ? encode_i64(ByteOrder::LITTLE_ENDIAN, [$physical]) : null,
            PhysicalType::FLOAT => is_float($physical) || is_int($physical)
                ? encode_f32(ByteOrder::LITTLE_ENDIAN, [(float) $physical])
                : null,
            PhysicalType::DOUBLE => is_float($physical) || is_int($physical)
                ? encode_f64(ByteOrder::LITTLE_ENDIAN, [(float) $physical])
                : null,
            PhysicalType::INT96, PhysicalType::FIXED_LEN_BYTE_ARRAY, PhysicalType::BYTE_ARRAY => is_string($physical)
                ? $physical
                : null,
        };

        return $encoded ?? throw new InvalidArgumentException(sprintf(
            'Column "%s": %s is not a %s physical value',
            $column->flatPath(),
            get_debug_type($physical),
            $column->type()->name,
        ));
    }

    /**
     * Null where the width varies (BYTE_ARRAY) or a bound may be truncated (FIXED_LEN_BYTE_ARRAY).
     */
    private function statisticWidth(FlatColumn $column): ?int
    {
        return match ($column->type()) {
            PhysicalType::BOOLEAN => 1,
            PhysicalType::INT32, PhysicalType::FLOAT => 4,
            PhysicalType::INT64, PhysicalType::DOUBLE => 8,
            PhysicalType::INT96 => 12,
            PhysicalType::FIXED_LEN_BYTE_ARRAY, PhysicalType::BYTE_ARRAY => null,
        };
    }
}
