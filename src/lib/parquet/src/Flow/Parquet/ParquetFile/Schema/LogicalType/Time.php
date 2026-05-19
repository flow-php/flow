<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema\LogicalType;

use Flow\Parquet\ThriftModel\TimeType;

final readonly class Time
{
    public function __construct(
        private bool $isAdjustedToUTC,
        private bool $millis,
        private bool $micros,
        private bool $nanos,
    ) {}

    public static function fromThrift(TimeType $timestamp): self
    {
        return new self(
            $timestamp->isAdjustedToUTC,
            // @mago-ignore analysis:redundant-comparison
            $timestamp->unit->MILLIS !== null,
            // @mago-ignore analysis:redundant-comparison
            $timestamp->unit->MICROS !== null,
            // @mago-ignore analysis:redundant-comparison
            $timestamp->unit->NANOS !== null,
        );
    }

    public function isAdjustedToUTC(): bool
    {
        return $this->isAdjustedToUTC;
    }

    public function micros(): bool
    {
        return $this->micros;
    }

    public function millis(): bool
    {
        return $this->millis;
    }

    public function nanos(): bool
    {
        return $this->nanos;
    }
}
