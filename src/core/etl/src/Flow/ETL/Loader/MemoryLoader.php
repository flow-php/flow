<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\ETL\Memory\Memory;

final readonly class MemoryLoader implements Loader
{
    public function __construct(private Memory $memory)
    {
    }

    #[\Override]
    public function load(Rows $rows, FlowContext $context) : void
    {
        $this->memory->save($rows->toArray());
    }
}
