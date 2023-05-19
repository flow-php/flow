<?php

declare(strict_types=1);

namespace Flow\ETL\Monitoring\Memory;

final class Consumption
{
    private readonly Unit $initial;

    private Unit $max;

    private Unit $min;

    public function __construct()
    {
        $this->initial = Unit::fromBytes(\memory_get_usage());
        $this->min = $this->initial;
        $this->max = $this->initial;
    }

    public function current() : Unit
    {
        $current = Unit::fromBytes(\memory_get_usage());

        if ($current->isGreaterThan($this->max)) {
            $this->max = $current;
        }

        if ($current->isLowerThan($this->min)) {
            $this->min = $current;
        }

        return $current;
    }

    public function currentDiff() : Unit
    {
        return $this->current()->diff($this->initial);
    }

    public function initial() : Unit
    {
        return $this->initial;
    }

    public function max() : Unit
    {
        return $this->max;
    }

    public function maxDiff() : Unit
    {
        return $this->max()->diff($this->initial());
    }

    public function min() : Unit
    {
        return $this->min;
    }

    public function minDiff() : Unit
    {
        return $this->min()->diff($this->initial());
    }
}
