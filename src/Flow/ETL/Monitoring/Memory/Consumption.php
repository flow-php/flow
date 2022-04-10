<?php

declare(strict_types=1);

namespace Flow\ETL\Monitoring\Memory;

final class Consumption
{
    private readonly Unit $initial;

    public function __construct()
    {
        $this->initial = Unit::fromBytes(\memory_get_usage());
    }

    public function current() : Unit
    {
        return Unit::fromBytes(\memory_get_usage());
    }

    public function currentDiff() : Unit
    {
        return $this->current()->diff($this->initial);
    }

    public function initial() : Unit
    {
        return $this->initial;
    }
}
