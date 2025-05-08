<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\{FlowContext, Loader, Rows};

final class SpyLoader implements Loader
{
    /**
     * @var array<Rows>
     */
    public array $loadedRows = [];

    public int $loadsCount = 0;

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
