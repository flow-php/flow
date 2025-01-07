<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Memory\Memory;
use Flow\ETL\{FlowContext, Loader, Rows};

final readonly class MemoryLoader implements Loader
{
    public function __construct(private Memory $memory)
    {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $this->memory->save($rows->toArray());
    }
}
