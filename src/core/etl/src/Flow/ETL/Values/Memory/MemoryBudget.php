<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Memory;

use Flow\ETL\Values\Column\Backend;

use function gc_mem_caches;

final class MemoryBudget
{
    private readonly Consumption $consumption;

    private int $releasedAt;

    public function __construct(
        Backend $backend,
        public readonly Unit $limit,
    ) {
        // live usage: real usage keeps freed chunks cached, so after a spill it would stay over the limit and every
        // later batch would spill on its own
        $this->consumption = new Consumption($backend, realMemory: false);
        $this->releasedAt = $this->consumption->capture()->inBytes();
    }

    public function exceeded(): bool
    {
        return $this->consumption->capture()->isGreaterThan($this->limit);
    }

    public function exceededWith(int $multiple): bool
    {
        $grown = $this->consumption->capture()->inBytes() - $this->releasedAt;

        return ($this->releasedAt + ($multiple * $grown)) > $this->limit->inBytes();
    }

    /**
     * After a spill freed its buffer: hands the memory PHP keeps cached back to the OS.
     */
    public function released(): void
    {
        gc_mem_caches();
        $this->releasedAt = $this->consumption->capture()->inBytes();
    }
}
