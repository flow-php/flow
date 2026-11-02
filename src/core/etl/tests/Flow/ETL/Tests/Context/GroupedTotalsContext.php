<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Functions\NumericTotal;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Column\GroupedTotals;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definitions;

use function Flow\Types\DSL\type_integer;

final readonly class GroupedTotalsContext
{
    /**
     * @param Definition<mixed> $definition
     */
    public function __construct(
        private Backend $backend,
        private Definition $definition,
    ) {}

    /**
     * @param list<array{0: list<mixed>, 1: list<int>}> $batches values and their group ids, batch by batch
     */
    public function grouped(array $batches): GroupedTotals
    {
        $totals = $this->backend->groupedTotals();

        foreach ($batches as [$values, $ids]) {
            $totals->add(
                $this->column($this->definition, $values),
                $this->column(Definitions::of('id', type_integer()), $ids),
            );
        }

        return $totals;
    }

    /**
     * One NumericTotal per group, fed each batch's rows of the group in row order.
     *
     * @param list<array{0: list<mixed>, 1: list<int>}> $batches
     *
     * @return list<NumericTotal> by group id
     */
    public function perGroup(array $batches): array
    {
        $totals = [];

        foreach ($batches as [$values, $ids]) {
            $column = $this->column($this->definition, $values);
            $rows = [];

            foreach ($ids as $row => $id) {
                $rows[$id][] = $row;
            }

            ksort($rows);

            foreach ($rows as $id => $indices) {
                $totals[$id] ??= new NumericTotal();
                $totals[$id]->aggregate($column, $indices);
            }
        }

        $byId = [];

        for ($id = 0, $groups = $totals === [] ? 0 : max(array_keys($totals)) + 1; $id < $groups; $id++) {
            $byId[] = $totals[$id] ?? new NumericTotal();
        }

        return $byId;
    }

    /**
     * @param Definition<mixed> $definition
     * @param list<mixed> $values
     */
    public function column(Definition $definition, array $values): Column
    {
        $builder = $this->backend->builder($definition);
        $builder->appendMany($values);

        return $builder->finish();
    }
}
