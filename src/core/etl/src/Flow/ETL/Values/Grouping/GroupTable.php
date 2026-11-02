<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Grouping;

use Flow\ETL\Values\Rows;

use function interface_exists;

if (interface_exists(GroupTable::class, false)) {
    return;
}

interface GroupTable
{
    public function assign(Rows $rows): GroupIds;

    public function count(): int;
}
