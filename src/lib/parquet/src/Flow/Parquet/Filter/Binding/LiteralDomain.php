<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use DateTimeImmutable;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Data\Converter\DecimalConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int32DateConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int64DateTimeConverter;
use Flow\Parquet\ParquetFile\Data\Converter\Int96DateTimeConverter;
use Flow\Parquet\ParquetFile\Data\Converter\JsonConverter;
use Flow\Parquet\ParquetFile\Data\Converter\UnsignedInt64Converter;
use Flow\Parquet\ParquetFile\Data\Converter\UnsignedIntegerConverter;
use Flow\Parquet\ParquetFile\Data\Converter\UuidConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function is_bool;
use function is_float;
use function is_int;
use function is_string;
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

    public static function forColumn(FlatColumn $column, ?Converter $converter): self
    {
        return match (true) {
            $converter instanceof Int32DateConverter,
            $converter instanceof Int64DateTimeConverter,
            $converter instanceof Int96DateTimeConverter,
                => self::DateTime,
            $converter instanceof DecimalConverter, $converter instanceof UnsignedIntegerConverter => self::Number,
            $converter instanceof UnsignedInt64Converter => self::UnsignedInteger64,
            $converter instanceof UuidConverter => self::Uuid,
            $converter instanceof JsonConverter => self::Bytes,
            $converter !== null => self::None,
            default => match ($column->type()) {
                PhysicalType::BOOLEAN => self::Boolean,
                PhysicalType::INT32, PhysicalType::INT64, PhysicalType::FLOAT, PhysicalType::DOUBLE => self::Number,
                PhysicalType::BYTE_ARRAY, PhysicalType::FIXED_LEN_BYTE_ARRAY => PhysicalOrder::forColumn($column)
                    === PhysicalOrder::Bytes
                        ? self::Bytes
                        : self::None,
                PhysicalType::INT96 => self::None,
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
     * The literal as the domain's values spell it: a uuid in lowercase, the form it decodes to.
     */
    public function normalise(bool|DateTimeImmutable|float|int|string $literal): bool|DateTimeImmutable|float|int|string
    {
        return $this === self::Uuid && is_string($literal) ? strtolower($literal) : $literal;
    }

    /**
     * @param list<bool|DateTimeImmutable|float|int|string> $literals
     */
    public function holdsEach(array $literals): bool
    {
        foreach ($literals as $literal) {
            if (!$this->holds($literal)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Whether a StartsWith prefix compares with the decoded values.
     */
    public function holdsPrefix(): bool
    {
        return $this === self::Bytes || $this === self::Uuid;
    }
}
