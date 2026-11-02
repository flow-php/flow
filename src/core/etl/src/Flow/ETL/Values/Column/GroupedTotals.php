<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use function interface_exists;

if (interface_exists(GroupedTotals::class, false)) {
    return;
}

interface GroupedTotals
{
    public function add(Column $values, Column $ids): void;

    public function sums(): Column;

    /**
     * @return list<array{count: int, sum: array{int: null|int, float: null|float, compensation: float}}>
     */
    public function partials(): array;

    /**
     * @param array<int, int> $into earlier group id => this group id
     */
    public function absorbEarlier(self $earlier, array $into): void;
}
