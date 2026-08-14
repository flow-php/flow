<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;

final class SpyLoader implements Closure, Loader
{
    public int $closureCount = 0;

    /**
     * @var array<FlowContext>
     */
    public array $contexts = [];

    /**
     * @var array<Rows>
     */
    public array $loadedRows = [];

    public int $loadsCount = 0;

    public function closure(FlowContext $context): void
    {
        $this->closureCount++;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->loadedRows[] = $rows;
        $this->contexts[] = $context;
        $this->loadsCount++;
    }

    public function reset(): void
    {
        $this->closureCount = 0;
        $this->contexts = [];
        $this->loadedRows = [];
        $this->loadsCount = 0;
    }
}
