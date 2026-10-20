<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use DateTimeImmutable;
use DateTimeInterface;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Binding\PreImage\BooleanPreImage;
use Flow\Parquet\Filter\Binding\PreImage\BytesPreImage;
use Flow\Parquet\Filter\Binding\PreImage\DatePreImage;
use Flow\Parquet\Filter\Binding\PreImage\DecimalPreImage;
use Flow\Parquet\Filter\Binding\PreImage\DoublePreImage;
use Flow\Parquet\Filter\Binding\PreImage\Float32PreImage;
use Flow\Parquet\Filter\Binding\PreImage\IntegerPreImage;
use Flow\Parquet\Filter\Binding\PreImage\TimestampPreImage;
use Flow\Parquet\Filter\Binding\PreImage\UnsignedInt64PreImage;
use Flow\Parquet\Filter\Binding\PreImage\UuidPreImage;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Data\Converter\DecimalConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int32DateConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int64DateTimeConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int96DateTimeConverter;
use Flow\Parquet\ParquetFile\Data\Converter\JsonConverter;
use Flow\Parquet\ParquetFile\Data\Converter\UnsignedInt32Converter;
use Flow\Parquet\ParquetFile\Data\Converter\UnsignedInt64Converter;
use Flow\Parquet\ParquetFile\Data\Converter\UuidConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function abs;
use function floor;
use function is_bool;
use function is_float;
use function is_int;
use function is_nan;
use function is_string;
use function pack;
use function preg_match;
use function strtolower;

/**
 * The literals a column's decoded values compare with; binding and evaluation both ask it.
 */
enum LiteralDomain
{
    case Boolean;
    case Bytes;
    case DateTime;
    case None;
    case Number;
    case UnsignedInteger64;
    case Uuid;

    /**
     * The largest magnitude up to which every integer is a double.
     */
    public const int EXACT_FLOAT_INTEGER = 2 ** 53;

    /**
     * The domain of the column's decoded values and their pre-image: null when no literal compares with them exactly.
     *
     * @return array{self, ?PreImage}
     */
    public static function forColumn(FlatColumn $column, ?Converter $converter): array
    {
        $logical = $column->logicalType();

        return match (true) {
            $converter instanceof Int32DateConverter => [self::DateTime, new DatePreImage()],
            $converter instanceof Int64DateTimeConverter => [self::DateTime, TimestampPreImage::forColumn($column)],
            $converter instanceof Int96DateTimeConverter => [self::DateTime, null],
            $converter instanceof DecimalConverter => [self::Number, DecimalPreImage::forColumn($column)],
            $converter instanceof UnsignedInt32Converter => [self::Number, new IntegerPreImage(0, 2 ** 32 - 1)],
            $converter instanceof UnsignedInt64Converter => [
                self::UnsignedInteger64,
                new UnsignedInt64PreImage($converter),
            ],
            $converter instanceof UuidConverter => [self::Uuid, new UuidPreImage($converter)],
            $converter instanceof JsonConverter => [self::Bytes, null],
            $converter !== null => [self::None, null],
            default => match ($column->type()) {
                PhysicalType::BOOLEAN => [self::Boolean, new BooleanPreImage()],
                PhysicalType::INT32, PhysicalType::INT64 => [self::Number, IntegerPreImage::forColumn($column)],
                PhysicalType::FLOAT => [self::Number, new Float32PreImage()],
                PhysicalType::DOUBLE => [self::Number, new DoublePreImage()],
                PhysicalType::BYTE_ARRAY, PhysicalType::FIXED_LEN_BYTE_ARRAY => PhysicalOrder::forColumn($column)
                    === PhysicalOrder::Bytes
                        ? [
                            self::Bytes,
                            $logical === null || $logical->is(LogicalType::STRING) || $logical->is(LogicalType::ENUM)
                                ? new BytesPreImage()
                                : null,
                        ]
                        : [self::None, null],
                PhysicalType::INT96 => [self::None, null],
            },
        };
    }

    public function holds(bool|DateTimeImmutable|float|int|string $literal): bool
    {
        return match ($this) {
            self::Boolean => is_bool($literal),
            self::Bytes => is_string($literal),
            self::DateTime => $literal instanceof DateTimeImmutable,
            self::None => false,
            self::Number => is_int($literal) || is_float($literal),
            self::UnsignedInteger64 => is_int($literal)
                || is_float($literal)
                || is_string($literal) && preg_match('/^(0|[1-9][0-9]*)$/', $literal) === 1,
            self::Uuid => is_string($literal)
                && preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/', $literal) === 1,
        };
    }

    /**
     * Values equal in the domain's order share a key (int 1 and float 1.0 alike), so IN looks a value up instead of
     * ordering it against each literal. Null orders it instead: NaN, a number beyond ±2^53 (where int and float
     * equality stops being transitive), a boolean, a float against UINT_64.
     */
    public function key(mixed $value): int|string|null
    {
        return match ($this) {
            self::Bytes, self::Uuid => is_string($value) ? $value : null,
            self::DateTime => $value instanceof DateTimeInterface ? $value->format('U.u') : null,
            self::Number => is_int($value) || is_float($value) ? self::numberKey($value) : null,
            self::UnsignedInteger64 => is_int($value) || is_string($value) ? (string) $value : null,
            self::Boolean, self::None => null,
        };
    }

    /**
     * The literal as the domain's values spell it: a uuid in lowercase, the form it decodes to.
     */
    public function normalise(bool|DateTimeImmutable|float|int|string $literal): bool|DateTimeImmutable|float|int|string
    {
        return $this === self::Uuid && is_string($literal) ? strtolower($literal) : $literal;
    }

    /**
     * Whether a StartsWith prefix compares with the decoded values.
     */
    public function holdsPrefix(): bool
    {
        return $this === self::Bytes || $this === self::Uuid;
    }

    private static function numberKey(float|int $value): int|string|null
    {
        return match (true) {
            is_nan($value), abs($value) > self::EXACT_FLOAT_INTEGER => null,
            is_int($value) => $value,
            $value === floor($value) => (int) $value,
            default => pack('e', $value),
        };
    }
}
