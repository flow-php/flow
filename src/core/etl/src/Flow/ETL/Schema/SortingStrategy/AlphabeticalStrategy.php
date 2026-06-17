<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\SortingStrategy;

use Flow\ETL\Row\SortOrder;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\SortingStrategy;

final readonly class AlphabeticalStrategy implements SortingStrategy
{
    public function __construct(
        private SortOrder $order = SortOrder::ASC,
    ) {}

    /**
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     */
    public function compare(Definition $left, Definition $right): int
    {
        if ($this->order === SortOrder::ASC) {
            return $left->entry()->name() <=> $right->entry()->name();
        }

        return $right->entry()->name() <=> $left->entry()->name();
    }
}
