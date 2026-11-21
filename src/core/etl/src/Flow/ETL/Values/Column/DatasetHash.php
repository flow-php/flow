<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column;

use function interface_exists;

if (interface_exists(DatasetHash::class, false)) {
    return;
}

interface DatasetHash
{
    /**
     * @param array<array-key, Column> $columns
     */
    public function add(array $columns, int $count): void;

    public function digest(): MultisetDigest;

    public function absorb(MultisetDigest $digest): void;
}
