<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Loader;
use Flow\ETL\Memory\Memory;
use Flow\ETL\Rows;

final class LoadIntoMemory implements Loader
{
    private Memory $memory;

    public function __construct(Memory $memory)
    {
        $this->memory = $memory;
    }

    public function load(Rows $rows) : void
    {
        $this->memory->save($rows->toArray());
    }
}
