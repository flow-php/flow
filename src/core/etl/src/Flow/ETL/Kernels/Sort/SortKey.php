<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Sort;

use function array_merge;
use function array_slice;

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

    /**
     * The keys of every slice in order, under the first slice's flag and direction.
     */
    public static function concat(self $first, self ...$rest): self
    {
        $ranks = [$first->ranks];
        $values = [$first->values];

        foreach ($rest as $key) {
            $ranks[] = $key->ranks;
            $values[] = $key->values;
        }

        return new self(array_merge(...$ranks), array_merge(...$values), $first->flag, $first->descending);
    }

    /**
     * The key of rows [$from, $from + $length).
     */
    public function slice(int $from, int $length): self
    {
        return new self(
            array_slice($this->ranks, $from, $length),
            array_slice($this->values, $from, $length),
            $this->flag,
            $this->descending,
        );
    }
}
