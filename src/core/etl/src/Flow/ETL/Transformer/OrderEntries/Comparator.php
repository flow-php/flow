<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\OrderEntries;

use Flow\ETL\Row\Entry;

interface Comparator
{
    /**
     * @param Entry<mixed, mixed> $left
     * @param Entry<mixed, mixed> $right
     *
     * @return int
     */
    public function compare(Entry $left, Entry $right) : int;
}
