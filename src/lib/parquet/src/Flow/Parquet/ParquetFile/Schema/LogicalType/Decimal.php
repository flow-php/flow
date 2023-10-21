<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema\LogicalType;

final class Decimal
{
    public function __construct(
        private readonly int $scale,
        private readonly int $precision
    ) {
    }

    public static function fromThrift(\Flow\Parquet\Thrift\DecimalType $thrift) : self
    {
        return new self(
            $thrift->scale,
            $thrift->precision
        );
    }

    public function precision() : int
    {
        return $this->precision;
    }

    public function scale() : int
    {
        return $this->scale;
    }
}
