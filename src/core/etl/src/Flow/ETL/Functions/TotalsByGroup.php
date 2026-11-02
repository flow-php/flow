<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedTotals;
use Flow\ETL\Values\Grouping\GroupIds;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema\Definitions;

use function Flow\Types\DSL\type_float;

final readonly class TotalsByGroup implements GroupedAggregation
{
    public function __construct(
        private AggregatesNumericTotal $aggregation,
        private GroupedTotals $totals,
    ) {}

    public function aggregateGroups(Rows $rows, GroupIds $ids, Backend $backend): void
    {
        $column = $this->aggregation->references()[0]->base();

        // a batch lacking the column still grows every group it holds, so each group id has a total
        $this->totals->add(
            $rows->schema()->findDefinition($column) === null
                ? $backend->constant(Definitions::of($column, type_float(), true), null, $rows->count())
                : $rows->column($column),
            $ids->ids,
        );
    }

    public function groupValues(int $groups, Backend $backend): Column
    {
        return $this->aggregation->groupValues($this->totals, $backend);
    }

    public function partials(int $groups): array
    {
        return $this->totals->partials();
    }

    public function absorbEarlier(GroupedAggregation $earlier, array $into, Backend $backend): void
    {
        if (!$earlier instanceof self) {
            throw new InvalidArgumentException('Grouped totals absorb only other grouped totals');
        }

        $this->totals->absorbEarlier($earlier->totals, $into);
    }
}
