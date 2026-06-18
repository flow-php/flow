<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\SortingStrategy;

use Flow\ETL\Row\SortOrder;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\SortingStrategy;
use Flow\ETL\Schema\SortingStrategy\TypeStrategy\TypePriorities;

final readonly class TypeStrategy implements SortingStrategy
{
    public function __construct(
        private TypePriorities $priorities = new TypePriorities(),
        private SortOrder $order = SortOrder::ASC,
    ) {}

    /**
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     */
    public function compare(Definition $left, Definition $right): int
    {
        $leftPriority = $this->priorities->for($left);
        $rightPriority = $this->priorities->for($right);

        return $this->order === SortOrder::ASC ? $leftPriority <=> $rightPriority : $rightPriority <=> $leftPriority;
    }
}
