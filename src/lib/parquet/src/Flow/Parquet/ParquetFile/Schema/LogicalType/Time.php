<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema\LogicalType;

use Flow\Parquet\Thrift\TimeType;

/**
 * @psalm-suppress RedundantConditionGivenDocblockType
 */
final class Time
{
    public function __construct(
        private readonly bool $isAdjustedToUTC,
        private readonly bool $millis,
        private readonly bool $micros,
        private readonly bool $nanos
    ) {
    }

    public static function fromThrift(TimeType $timestamp) : self
    {
        return new self(
            $timestamp->isAdjustedToUTC,
            $timestamp->unit->MILLIS !== null,
            $timestamp->unit->MICROS !== null,
            $timestamp->unit->NANOS !== null
        );
    }

    public function isAdjustedToUTC() : bool
    {
        return $this->isAdjustedToUTC;
    }

    public function micros() : bool
    {
        return $this->micros;
    }

    public function millis() : bool
    {
        return $this->millis;
    }

    public function nanos() : bool
    {
        return $this->nanos;
    }
}
