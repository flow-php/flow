<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

/**
 * One reference's sort key over a batch: a rank per row (0 null, 1 value, 2 NaN) and the value it orders by within
 * rank 1, compared with $flag (SORT_NUMERIC, SORT_STRING or SORT_REGULAR).
 */
final readonly class SortKey
{
    /**
     * @param list<int> $ranks
     * @param list<mixed> $values
     */
    public function __construct(
        public array $ranks,
        public array $values,
        public int $flag,
        public bool $descending,
    ) {}
}
