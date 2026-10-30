<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema\SortingStrategy;

use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\SortingStrategy;
use Flow\ETL\Values\Schema\SortingStrategy\TypeStrategy\TypePriorities;
use Flow\ETL\Values\SortOrder;

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
