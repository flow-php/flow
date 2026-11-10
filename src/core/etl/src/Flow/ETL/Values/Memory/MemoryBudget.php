<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Memory;

use Flow\ETL\Values\Column\Backend;

use function intdiv;
use function min;

final class MemoryBudget
{
    public const int FLOOR_CAP_BYTES = 134_217_728;

    public const int FLOOR_DIVISOR = 16;

    private readonly Consumption $consumption;

    private int $baseline;

    public function __construct(
        Backend $backend,
        public readonly Unit $limit,
        public readonly ?Unit $processCap,
    ) {
        // live usage: real usage keeps freed chunks cached, so after a spill it would stay over the limit and every
        // later batch would spill on its own
        $this->consumption = new Consumption($backend, realMemory: false);
        $this->baseline = $this->consumption->capture()->inBytes();
    }

    public static function of(Backend $backend, Unit $limit): self
    {
        return new self($backend, $limit, MemoryLimit::processCap());
    }

    public function exceeded(): bool
    {
        return $this->exceededWith(1);
    }

    public function exceededWith(int $multiple): bool
    {
        $process = $this->consumption->capture()->inBytes();
        $grown = $process - $this->baseline;
        $own = $multiple * $grown;

        return (
            $own > $this->limit->inBytes()
            || $this->crossesCapWith(($multiple - 1) * $grown)
            && $own > $this->floor()
        );
    }

    public function remaining(): int
    {
        $process = $this->consumption->capture()->inBytes();
        $allowance = $this->crossesCapWith(0) ? min($this->limit->inBytes(), $this->floor()) : $this->limit->inBytes();

        return $allowance - ($process - $this->baseline);
    }

    /** After a spill freed its buffer: growth is measured from here. */
    public function released(): void
    {
        $this->baseline = $this->consumption->capture()->inBytes();
    }

    public function crossesCapWith(int $bytes): bool
    {
        return (
            $this->processCap !== null
            && ($this->consumption->capture()->inBytes() + $bytes) > $this->processCap->inBytes()
        );
    }

    public function floor(): int
    {
        return $this->processCap === null
            ? $this->limit->inBytes()
            : min(self::FLOOR_CAP_BYTES, intdiv($this->processCap->inBytes(), self::FLOOR_DIVISOR));
    }
}
