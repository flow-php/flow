<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedCounts;
use Flow\ETL\Values\Grouping\GroupIds;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema\Definitions;

use function array_map;
use function Flow\Types\DSL\type_string;

final readonly class CountsByGroup implements GroupedAggregation
{
    public function __construct(
        private ?string $column,
        private GroupedCounts $counts,
    ) {}

    public function aggregateGroups(Rows $rows, GroupIds $ids, Backend $backend): void
    {
        $this->counts->add(
            match (true) {
                // count() without a column counts rows, and a group id is never null
                $this->column === null => $ids->ids,
                $rows->schema()->findDefinition($this->column) === null => $backend->constant(
                    Definitions::of($this->column, type_string(), true),
                    null,
                    $rows->count(),
                ),
                default => $rows->column($this->column),
            },
            $ids->ids,
        );
    }

    public function groupValues(int $groups, Backend $backend): Column
    {
        return $this->counts->counts();
    }

    public function partials(int $groups): array
    {
        return array_map(static fn(int $count): array => ['count' => $count], $this->counts->partials());
    }

    public function absorbEarlier(GroupedAggregation $earlier, array $into, Backend $backend): void
    {
        if (!$earlier instanceof self) {
            throw new InvalidArgumentException('Grouped counts absorb only other grouped counts');
        }

        $this->counts->absorbEarlier($earlier->counts, $into);
    }
}
