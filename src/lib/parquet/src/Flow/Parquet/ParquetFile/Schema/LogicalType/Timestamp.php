<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema\LogicalType;

use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\ThriftModel\TimestampType;

final readonly class Timestamp
{
    public function __construct(
        private bool $isAdjustedToUTC,
        private TimeUnit $unit,
    ) {}

    public static function fromThrift(TimestampType $timestamp): self
    {
        return new self($timestamp->isAdjustedToUTC, TimeUnit::fromThrift($timestamp->unit));
    }

    public function isAdjustedToUTC(): bool
    {
        return $this->isAdjustedToUTC;
    }

    public function micros(): bool
    {
        return $this->unit === TimeUnit::MICROSECONDS;
    }

    public function millis(): bool
    {
        return $this->unit === TimeUnit::MILLISECONDS;
    }

    public function nanos(): bool
    {
        return $this->unit === TimeUnit::NANOSECONDS;
    }

    public function unit(): TimeUnit
    {
        return $this->unit;
    }
}
