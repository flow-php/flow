<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ThriftModel\ColumnOrder as ThriftColumnOrder;
use Flow\Parquet\ThriftModel\IEEE754TotalOrder;
use Flow\Parquet\ThriftModel\Int96TimestampOrder;
use Flow\Parquet\ThriftModel\TypeDefinedOrder;

enum ColumnOrder
{
    case Absent;
    case Ieee754TotalOrder;
    case Int96Timestamp;
    case TypeDefined;
    case Unknown;

    public static function forWriting(FlatColumn $column): self
    {
        if (PhysicalOrder::forColumn($column)->isFloatingPoint()) {
            return self::Ieee754TotalOrder;
        }

        if ($column->type() === PhysicalType::INT96) {
            return self::Int96Timestamp;
        }

        return self::TypeDefined;
    }

    public static function fromThrift(?ThriftColumnOrder $order): self
    {
        if ($order === null) {
            return self::Absent;
        }

        return match (true) {
            $order->TYPE_ORDER !== null => self::TypeDefined,
            $order->IEEE_754_TOTAL_ORDER !== null => self::Ieee754TotalOrder,
            $order->INT96_TIMESTAMP_ORDER !== null => self::Int96Timestamp,
            default => self::Unknown,
        };
    }

    public function isTrusted(FlatColumn $column): bool
    {
        if (PhysicalOrder::forColumn($column)->isUnordered()) {
            return false;
        }

        return match ($this) {
            self::TypeDefined, self::Ieee754TotalOrder => true,
            self::Absent, self::Int96Timestamp, self::Unknown => false,
        };
    }

    public function toThrift(): ThriftColumnOrder
    {
        return match ($this) {
            self::TypeDefined => new ThriftColumnOrder(['TYPE_ORDER' => new TypeDefinedOrder()]),
            self::Ieee754TotalOrder => new ThriftColumnOrder(['IEEE_754_TOTAL_ORDER' => new IEEE754TotalOrder()]),
            self::Int96Timestamp => new ThriftColumnOrder(['INT96_TIMESTAMP_ORDER' => new Int96TimestampOrder()]),
            self::Unknown => new ThriftColumnOrder(),
            self::Absent => throw new InvalidArgumentException('Column order Absent cannot be written'),
        };
    }

    /**
     * The statistics max a reader may use, null when it may not. Under TYPE_DEFINED_ORDER a floating point max of NaN
     * is ignored and a -0.0 max may hide +0.0 values (parquet.thrift ColumnOrder).
     */
    public function trustedMax(FlatColumn $column, bool|float|int|string|null $max): bool|float|int|string|null
    {
        if ($max === null || !$this->isTrusted($column)) {
            return null;
        }

        $order = PhysicalOrder::forColumn($column);

        if ($this !== self::TypeDefined || !$order->isFloatingPoint()) {
            return $max;
        }

        return match (true) {
            $order->isNan($max) => null,
            $order->isZero($max) => $order->zero(negative: false),
            default => $max,
        };
    }

    /**
     * The statistics min a reader may use, null when it may not. Under TYPE_DEFINED_ORDER a floating point min of NaN
     * is ignored and a +0.0 min may hide -0.0 values (parquet.thrift ColumnOrder).
     */
    public function trustedMin(FlatColumn $column, bool|float|int|string|null $min): bool|float|int|string|null
    {
        if ($min === null || !$this->isTrusted($column)) {
            return null;
        }

        $order = PhysicalOrder::forColumn($column);

        if ($this !== self::TypeDefined || !$order->isFloatingPoint()) {
            return $min;
        }

        return match (true) {
            $order->isNan($min) => null,
            $order->isZero($min) => $order->zero(negative: true),
            default => $min,
        };
    }
}
