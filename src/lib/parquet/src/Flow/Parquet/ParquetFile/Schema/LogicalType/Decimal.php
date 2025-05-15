<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema\LogicalType;

use Flow\Parquet\Thrift\DecimalType;

final readonly class Decimal
{
    public function __construct(
        private int $scale,
        private int $precision,
    ) {
    }

    public static function fromThrift(DecimalType $thrift) : self
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
