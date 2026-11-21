<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use function interface_exists;

if (interface_exists(GroupedMultisetHashes::class, false)) {
    return;
}

interface GroupedMultisetHashes
{
    /**
     * @param array<array-key, Column> $columns
     */
    public function add(array $columns, Column $ids): void;

    public function hashes(): Column;

    /**
     * @return list<MultisetDigest> by group id
     */
    public function partials(): array;

    /**
     * @param array<int, int> $into earlier group id => this group id
     */
    public function absorbEarlier(self $earlier, array $into): void;
}
