<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedMultisetHashes;
use Flow\ETL\Values\Column\MultisetDigest;
use Flow\ETL\Values\Grouping\GroupIds;
use Flow\ETL\Values\Rows;

use function array_combine;
use function array_map;

final readonly class MultisetHashesByGroup implements GroupedAggregation
{
    /**
     * @param null|list<string> $columns null takes every column of each batch
     */
    public function __construct(
        private ?array $columns,
        private GroupedMultisetHashes $hashes,
    ) {}

    public function aggregateGroups(Rows $rows, GroupIds $ids, Backend $backend): void
    {
        if ($rows->isEmpty()) {
            return;
        }

        $this->hashes->add(
            $this->columns === null
                ? $rows->columns()
                : array_combine($this->columns, array_map($rows->column(...), $this->columns)),
            $ids->ids,
        );
    }

    public function groupValues(int $groups, Backend $backend): Column
    {
        return $this->hashes->hashes();
    }

    public function partials(int $groups): array
    {
        return array_map(static fn(MultisetDigest $digest): array => $digest->normalize(), $this->hashes->partials());
    }

    public function absorbEarlier(GroupedAggregation $earlier, array $into, Backend $backend): void
    {
        if (!$earlier instanceof self) {
            throw new InvalidArgumentException('Grouped multiset hashes absorb only other grouped multiset hashes');
        }

        $this->hashes->absorbEarlier($earlier->hashes, $into);
    }
}
