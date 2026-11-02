<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use function interface_exists;

if (interface_exists(GroupedCounts::class, false)) {
    return;
}

interface GroupedCounts
{
    public function add(Column $values, Column $ids): void;

    public function counts(): Column;

    /**
     * @return list<int> by group id
     */
    public function partials(): array;

    /**
     * @param array<int, int> $into earlier group id => this group id
     */
    public function absorbEarlier(self $earlier, array $into): void;
}
