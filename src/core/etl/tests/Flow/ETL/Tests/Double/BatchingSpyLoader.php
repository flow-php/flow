<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\{FlowContext, Loader, Loader\BatchingLoader, Rows};

final class BatchingSpyLoader implements BatchingLoader, Loader
{
    /**
     * @var array<Rows>
     */
    public array $loadedRows = [];

    public int $loadsCount = 0;

    public function defaultBatchSize() : int
    {
        return 100;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $this->loadedRows[] = $rows;
        $this->loadsCount++;
    }

    public function reset() : void
    {
        $this->loadedRows = [];
        $this->loadsCount = 0;
    }
}
