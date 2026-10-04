<?php

declare(strict_types=1);

namespace Flow\ETL\Dataset\Memory;

use Flow\ETL\Column\Backend;

use function gc_mem_caches;

final readonly class MemoryBudget
{
    private Consumption $consumption;

    public function __construct(
        Backend $backend,
        public Unit $limit,
    ) {
        // live usage: real usage keeps freed chunks cached, so after a spill it would stay over the limit and every
        // later batch would spill on its own
        $this->consumption = new Consumption($backend, realMemory: false);
    }

    public function exceeded(): bool
    {
        return $this->consumption->capture()->isGreaterThan($this->limit);
    }

    /**
     * After a spill freed its buffer: hands the memory PHP keeps cached back to the OS.
     */
    public function released(): void
    {
        gc_mem_caches();
    }
}
