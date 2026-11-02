<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Grouping\GroupIds;
use Flow\ETL\Values\Rows;

interface GroupedAggregation
{
    public function aggregateGroups(Rows $rows, GroupIds $ids, Backend $backend): void;

    public function groupValues(int $groups, Backend $backend): Column;

    /**
     * @return list<array<string, mixed>> by group id
     */
    public function partials(int $groups): array;

    /**
     * @param array<int, int> $into earlier group id => this group id
     */
    public function absorbEarlier(self $earlier, array $into, Backend $backend): void;
}
