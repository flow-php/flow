<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedMultisetHashes;
use Flow\ETL\Values\Grouping\GroupIds;
use Flow\ETL\Values\Rows;

use function array_map;

final readonly class MultisetHashesByGroup implements GroupedAggregation
{
    /**
     * @param list<string> $columns [] takes every column of each batch
     */
    public function __construct(
        private array $columns,
        private GroupedMultisetHashes $hashes,
    ) {}

    public function aggregateGroups(Rows $rows, GroupIds $ids, Backend $backend): void
    {
        if ($rows->isEmpty()) {
            return;
        }

        $this->hashes->add(
            $this->columns === [] ? $rows->columns() : $rows->select(...$this->columns)->columns(),
            $ids->ids,
        );
    }

    public function groupValues(int $groups, Backend $backend): Column
    {
        return $this->hashes->hashes();
    }

    public function partials(int $groups): array
    {
        return array_map(static fn(mixed $hash): array => ['hash' => $hash], $this->hashes->hashes()->physicals());
    }

    public function absorbEarlier(GroupedAggregation $earlier, array $into, Backend $backend): void
    {
        if (!$earlier instanceof self) {
            throw new InvalidArgumentException('Grouped multiset hashes absorb only other grouped multiset hashes');
        }

        $this->hashes->absorbEarlier($earlier->hashes, $into);
    }
}
