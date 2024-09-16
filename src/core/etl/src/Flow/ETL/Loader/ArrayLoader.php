<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\{FlowContext, Loader, Rows};

final class ArrayLoader implements Loader
{
    public function __construct(private array &$memory)
    {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $this->memory = \array_merge(
            $this->memory,
            $rows->toArray()
        );
    }
}
