<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Grouping\GroupIds;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema\Definition;

use function array_key_exists;

final class PerGroupAggregation implements GroupedAggregation
{
    /**
     * @var array<int, AggregatingFunction> by group id
     */
    private array $groups = [];

    public function __construct(
        private readonly AggregatingFunction $aggregation,
        private readonly Definition $output,
    ) {}

    public function aggregateGroups(Rows $rows, GroupIds $ids, Backend $backend): void
    {
        $rowsOf = [];

        /** @var int $id */
        foreach ($ids->ids->physicals() as $row => $id) {
            $rowsOf[$id][] = $row;
        }

        foreach ($rowsOf as $id => $groupRows) {
            ($this->groups[$id] ??= $this->fresh())->aggregate($rows, $groupRows, $backend);
        }
    }

    public function groupValues(int $groups, Backend $backend): Column
    {
        $values = $backend->builder($this->output);

        for ($id = 0; $id < $groups; $id++) {
            $values->append(($this->groups[$id] ?? $this->fresh())->value());
        }

        return $values->finish();
    }

    public function partials(int $groups): array
    {
        $partials = [];

        for ($id = 0; $id < $groups; $id++) {
            $partials[] = ($this->groups[$id] ?? $this->fresh())->partial();
        }

        return $partials;
    }

    public function absorbEarlier(GroupedAggregation $earlier, array $into, Backend $backend): void
    {
        if (!$earlier instanceof self) {
            throw new InvalidArgumentException('A per-group aggregation absorbs only another per-group aggregation');
        }

        foreach ($into as $from => $to) {
            $absorbed = $earlier->groups[$from] ?? $this->fresh();

            if (array_key_exists($to, $this->groups)) {
                $absorbed->merge($this->groups[$to]->partial(), $backend);
            }

            $this->groups[$to] = $absorbed;
        }
    }

    private function fresh(): AggregatingFunction
    {
        return $this->aggregation->withChildren($this->aggregation->children());
    }
}
