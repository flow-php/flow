<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\{FlowContext, Loader, Rows};

final class ArrayLoader implements Loader
{
    /**
     * @param array<array<mixed>> $array
     */
    public function __construct(private array &$array)
    {
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $this->array = \array_merge(
            $this->array,
            $rows->toArray()
        );
    }
}
