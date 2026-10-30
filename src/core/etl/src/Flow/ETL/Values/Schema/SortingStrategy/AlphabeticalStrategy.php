<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema\SortingStrategy;

use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\SortingStrategy;
use Flow\ETL\Values\SortOrder;

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
            return $left->name() <=> $right->name();
        }

        return $right->name() <=> $left->name();
    }
}
