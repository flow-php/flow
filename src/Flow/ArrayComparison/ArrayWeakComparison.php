<?php

declare(strict_types=1);

namespace Flow\ArrayComparison;

/**
 * @psalm-immutable
 */
final class ArrayWeakComparison
{
    /**
     * @phpstan-ignore-next-line
     */
    public function equals(array $a, array $b) : bool
    {
        return $this->compare($a, $b) === 0;
    }

    /**
     * @phpstan-ignore-next-line
     */
    public function compare(array $a, array $b) : int
    {
        return (new ArraySortByKey)($a) <=> (new ArraySortByKey)($b);
    }
}
