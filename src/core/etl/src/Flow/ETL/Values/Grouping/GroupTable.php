<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Grouping;

use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;

use function interface_exists;

if (interface_exists(GroupTable::class, false)) {
    return;
}

interface GroupTable
{
    public function assign(Rows $rows): GroupIds;

    /**
     * @return Column the group of each row as a nullable integer, null for a key the table never saw
     */
    public function lookup(Rows $rows): Column;

    public function count(): int;
}
