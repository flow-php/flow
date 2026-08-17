<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;

use function array_map;
use function array_merge;

final class SpyLoader implements Closure, Loader
{
    /**
     * @var array<FlowContext>
     */
    public array $closureContexts = [];

    public int $closureCount = 0;

    /**
     * @var array<FlowContext>
     */
    public array $contexts = [];

    /**
     * @var list<Rows>
     */
    public array $loadedRows = [];

    public int $loadsCount = 0;

    public function closure(FlowContext $context): void
    {
        $this->closureContexts[] = $context;
        $this->closureCount++;
    }

    /**
     * @return list<int>
     */
    public function loadedRowCounts(): array
    {
        return array_map(static fn(Rows $rows): int => $rows->count(), $this->loadedRows);
    }

    /**
     * @return array<int, array<array-key, mixed>>
     */
    public function loadedRowsToArray(): array
    {
        return array_merge(...array_map(static fn(Rows $rows): array => $rows->toArray(), $this->loadedRows));
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->loadedRows[] = $rows;
        $this->contexts[] = $context;
        $this->loadsCount++;
    }

    public function reset(): void
    {
        $this->closureContexts = [];
        $this->closureCount = 0;
        $this->contexts = [];
        $this->loadedRows = [];
        $this->loadsCount = 0;
    }
}
