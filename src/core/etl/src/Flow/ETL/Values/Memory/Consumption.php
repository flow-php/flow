<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Memory;

use Flow\ETL\Values\Column\Backend;

use function memory_get_usage;

final class Consumption
{
    private readonly Unit $initial;

    private Unit $max;

    private Unit $min;

    public function __construct(
        private readonly Backend $backend,
        private readonly bool $realMemory = true,
    ) {
        $this->initial = Unit::fromBytes(memory_get_usage($this->realMemory) + $this->backend->allocatedBytes());
        $this->min = $this->initial;
        $this->max = $this->initial;
    }

    public function capture(): Unit
    {
        $current = Unit::fromBytes(memory_get_usage($this->realMemory) + $this->backend->allocatedBytes());

        if ($current->isGreaterThan($this->max)) {
            $this->max = $current;
        }

        if ($current->isLowerThan($this->min)) {
            $this->min = $current;
        }

        return $current;
    }

    public function currentDiff(): Unit
    {
        return $this->capture()->diff($this->initial);
    }

    public function initial(): Unit
    {
        return $this->initial;
    }

    public function max(): Unit
    {
        return $this->max;
    }

    public function maxDiff(): Unit
    {
        return $this->max()->diff($this->initial());
    }

    public function min(): Unit
    {
        return $this->min;
    }

    public function minDiff(): Unit
    {
        return $this->min()->diff($this->initial());
    }

    /**
     * @return array{initial: int, min: int, max: int} in bytes
     */
    public function normalize(): array
    {
        return ['initial' => $this->initial->inBytes(), 'min' => $this->min->inBytes(), 'max' => $this->max->inBytes()];
    }
}
