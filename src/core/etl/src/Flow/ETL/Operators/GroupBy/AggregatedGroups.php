<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Functions\AggregatesByGroup;
use Flow\ETL\Functions\AggregatesNumericTotal;
use Flow\ETL\Functions\GroupedAggregation;
use Flow\ETL\Functions\PerGroupAggregation;
use Flow\ETL\Functions\TotalsByGroup;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Grouping\GroupTable;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function array_combine;
use function array_key_exists;
use function array_map;
use function array_push;
use function array_values;
use function count;

final class AggregatedGroups
{
    private readonly GroupTable $table;

    /**
     * @var list<GroupedAggregation> one per aggregator, in the shape's order
     */
    private readonly array $outputs;

    /**
     * @var list<GroupedAggregation> the distinct ones, each fed every batch once
     */
    private readonly array $accumulating;

    /**
     * @var array<string, list<mixed>> by key column, by group id
     */
    private array $keys = [];

    /**
     * @var array<array-key, true> by position: the aggregators whose output is the wrapped aggregate's partials
     */
    private readonly array $partialPositions;

    public function __construct(
        private readonly GroupBy $groupBy,
        private readonly GroupByShape $shape,
        Backend $backend,
    ) {
        $this->table = $groupBy->groupTable($shape->input, $backend);
        $outputs = [];
        $accumulating = [];
        $totals = [];
        $partialPositions = [];

        foreach ($shape->aggregators as $position => $aggregator) {
            // a split's partial accumulates as the aggregate it wraps, so both share one grouped path
            if ($aggregator instanceof PartialAggregate) {
                $aggregator = $aggregator->aggregate;
                $partialPositions[$position] = true;
            }

            if ($aggregator instanceof AggregatesNumericTotal && !$aggregator->exact()) {
                // a sum and an average of one column share one inexact total, so the column is added once
                $shared = 'total-' . $aggregator->references()[0]->base();
                $totals[$shared] ??= $backend->groupedTotals();
                $output = new TotalsByGroup($aggregator, $totals[$shared]);

                if (!array_key_exists($shared, $accumulating)) {
                    $accumulating[$shared] = $output;
                }
            } else {
                $output = $aggregator instanceof AggregatesByGroup
                    ? $aggregator->grouped($backend)
                    : new PerGroupAggregation($aggregator, $shape->output->get($aggregator->outputName()));
                $accumulating[] = $output;
            }

            $outputs[] = $output;
        }

        $this->outputs = $outputs;
        $this->partialPositions = $partialPositions;
        $this->accumulating = array_values($accumulating);

        foreach ($groupBy->refs()->all() as $ref) {
            $this->keys[$ref->to()] = [];
        }
    }

    public function accumulate(Rows $batch, Backend $backend): void
    {
        $ids = $this->table->assign($batch);

        foreach ($this->groupBy->keyValues($batch, $this->shape->input, $ids->firstRows) as $name => $values) {
            array_push($this->keys[$name], ...$values);
        }

        foreach ($this->accumulating as $aggregation) {
            $aggregation->aggregateGroups($batch, $ids, $backend);
        }
    }

    public function count(): int
    {
        return $this->table->count();
    }

    /**
     * @param list<int> $groups
     */
    public function keyRows(array $groups, Backend $backend): Rows
    {
        $columns = [];
        $definitions = [];

        foreach ($this->keys as $name => $values) {
            $definitions[] = $definition = $this->shape->input->get($name);
            $column = $backend->builder($definition);
            $column->appendMany(array_map(static fn(int $group): mixed => $values[$group], $groups));
            $columns[$name] = $column->finish();
        }

        return Rows::fromColumns(new Schema(...$definitions), $columns, count($groups));
    }

    /**
     * @param list<int> $groups the ids in $earlier to absorb, whose rows came before every row of this one
     */
    public function absorbEarlier(self $earlier, array $groups, Backend $backend): void
    {
        if ($groups === []) {
            return;
        }

        $ids = $this->table->assign($earlier->keyRows($groups, $backend));

        foreach ($ids->firstRows as $row) {
            foreach ($earlier->keys as $name => $values) {
                $this->keys[$name][] = $values[$groups[$row]];
            }
        }

        /** @var list<int> $targets */
        $targets = $ids->ids->physicals();
        $into = array_combine($groups, $targets);

        foreach ($this->accumulating as $position => $aggregation) {
            $aggregation->absorbEarlier($earlier->accumulating[$position], $into, $backend);
        }
    }

    /**
     * @param int<1, max> $batchSize
     *
     * @return Generator<Rows>
     */
    public function flush(int $batchSize, Backend $backend): Generator
    {
        if ($this->count() > 0) {
            yield from $this->rows($this->count(), $backend)->chunks($batchSize);
        }
    }

    public function noInputRow(Backend $backend): Rows
    {
        $columns = [];

        foreach ($this->shape->aggregators->fresh() as $aggregator) {
            $definition = $this->shape->output->get($aggregator->outputName());
            $column = $backend->builder($definition);
            $column->append($aggregator->value());
            $columns[$definition->name()] = $column->finish();
        }

        return Rows::fromColumns($this->shape->output, $columns, 1);
    }

    private function rows(int $groups, Backend $backend): Rows
    {
        $output = $this->shape->output;
        $columns = [];

        foreach ($this->keys as $name => $values) {
            $definition = $output->get($name);
            $column = $backend->builder($definition);
            $column->appendMany($values);
            $columns[$definition->name()] = $column->finish();
        }

        foreach ($this->shape->aggregators as $position => $aggregator) {
            if (!array_key_exists($position, $this->partialPositions)) {
                $columns[$aggregator->outputName()] = $this->outputs[$position]->groupValues($groups, $backend);

                continue;
            }

            $partials = $backend->builder($output->get($aggregator->outputName()));
            $partials->appendMany($this->outputs[$position]->partials($groups));
            $columns[$aggregator->outputName()] = $partials->finish();
        }

        return Rows::fromColumns($output, $columns, $groups);
    }
}
