<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\SortingStrategy;

use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\SortingStrategy;

final readonly class CombinedStrategy implements SortingStrategy
{
    public function __construct(
        private SortingStrategy $first,
        private SortingStrategy $second,
    ) {}

    /**
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     */
    public function compare(Definition $left, Definition $right): int
    {
        $result = $this->first->compare($left, $right);

        if ($result === 0) {
            return $this->second->compare($left, $right);
        }

        return $result;
    }
}
