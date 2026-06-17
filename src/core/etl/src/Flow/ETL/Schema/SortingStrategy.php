<?php

declare(strict_types=1);

namespace Flow\ETL\Schema;

interface SortingStrategy
{
    /**
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     */
    public function compare(Definition $left, Definition $right): int;
}
