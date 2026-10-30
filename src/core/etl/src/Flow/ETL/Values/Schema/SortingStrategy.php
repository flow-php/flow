<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema;

interface SortingStrategy
{
    /**
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     */
    public function compare(Definition $left, Definition $right): int;
}
