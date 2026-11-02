<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Grouping;

use Flow\ETL\Values\Column\Column;

final readonly class GroupIds
{
    /**
     * @param list<int> $firstRows the row of each new group, by new id
     */
    public function __construct(
        public Column $ids,
        public array $firstRows,
    ) {}
}
