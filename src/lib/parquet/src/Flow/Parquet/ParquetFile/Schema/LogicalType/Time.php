<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema\LogicalType;

use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\ThriftModel\TimeType;

final readonly class Time
{
    public function __construct(
        private bool $isAdjustedToUTC,
        private TimeUnit $unit,
    ) {}

    public static function fromThrift(TimeType $time): self
    {
        return new self($time->isAdjustedToUTC, TimeUnit::fromThrift($time->unit));
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
