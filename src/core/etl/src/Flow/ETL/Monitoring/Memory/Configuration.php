<?php

declare(strict_types=1);

namespace Flow\ETL\Monitoring\Memory;

use Flow\ETL\Exception\InvalidArgumentException;

final class Configuration
{
    private ?Unit $limit = null;

    public function __construct(int $safetyBufferPercentage)
    {
        if ($safetyBufferPercentage < 0 || $safetyBufferPercentage > 90) {
            throw new InvalidArgumentException("Safety buffer can't be smaller than 0% and greater than 90%, {$safetyBufferPercentage}% given.");
        }

        $limitConfig = \ini_get('memory_limit');

        if ($limitConfig !== false && !\str_starts_with($limitConfig, '-')) {
            $this->limit = Unit::fromString($limitConfig)->percentage(100 - $safetyBufferPercentage);
        }
    }

    public function isConsumptionBelow(Unit $unit, int $limitPercentage) : bool
    {
        // if memory is unlimited then current consumption is always below certain threshold
        if ($this->limit === null) {
            return true;
        }

        return (($unit->inBytes() / $this->limit->inBytes()) * 100) < $limitPercentage;
    }

    public function isInfinite() : bool
    {
        return $this->limit === null;
    }

    public function isLessThan(Unit $memory) : bool
    {
        if ($this->limit === null) {
            return false;
        }

        return $this->limit->inBytes() < $memory->inBytes();
    }

    public function limit() : ?Unit
    {
        return $this->limit;
    }
}
