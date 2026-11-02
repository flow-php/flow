<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Grouping\GroupTable;
use Flow\ETL\Values\Rows;
use Generator;

use function array_filter;
use function array_keys;
use function array_push;
use function count;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function min;

final class PivotedTable
{
    /**
     * @var array<int, array<array-key, AggregatingFunction>> group id => pivot value => accumulator
     */
    private array $accumulators = [];

    /**
     * @var array<string, list<mixed>> by key column, by group id
     */
    private array $keys = [];

    private ?GroupTable $table = null;

    public function __construct(
        private readonly GroupBy $groupBy,
        private readonly PivotShape $shape,
    ) {
        foreach ($groupBy->refs()->all() as $ref) {
            $this->keys[$ref->to()] = [];
        }
    }

    public function accumulate(Rows $batch, Backend $backend): void
    {
        if ($batch->isEmpty()) {
            return;
        }

        $this->table ??= $this->groupBy->groupTable($this->shape->input, $backend);
        $ids = $this->table->assign($batch);

        foreach ($this->groupBy->keyValues($batch, $this->shape->input, $ids->firstRows) as $name => $values) {
            array_push($this->keys[$name], ...$values);
        }

        $pivotValues = $batch->column($this->shape->pivot->column->base())->values();
        // a column of ints or strings is checked once, in C-speed passes; anything else is asserted row by row
        $scalars =
            (
                count(array_filter($pivotValues, 'is_int')) + count(array_filter(
                    $pivotValues,
                    'is_string',
                )) + count(array_keys($pivotValues, null, true))
            ) === count($pivotValues);
        $pivotType = type_union(type_string(), type_integer());

        /** @var array<int, array<array-key, list<int>>> $indices */
        $indices = [];

        /** @var int $index */
        foreach ($ids->ids->physicals() as $i => $index) {
            $this->accumulators[$index] ??= [];

            if ($pivotValues[$i] === null) {
                continue;
            }

            /** @var int|string $column */
            $column = $scalars ? $pivotValues[$i] : $pivotType->assert($pivotValues[$i]);
            $indices[$index][$column][] = $i;
        }

        foreach ($indices as $index => $columns) {
            foreach ($columns as $column => $rows) {
                ($this->accumulators[$index][$column] ??= $this->shape->aggregation->withChildren(
                    $this->shape->aggregation->children(),
                ))->aggregate($batch, $rows, $backend);
            }
        }
    }

    /**
     * @return Generator<Rows>
     */
    public function flush(BatchSize $batchSize, Backend $backend): Generator
    {
        $groups = $this->table?->count() ?? 0;

        for ($first = 0; $first < $groups; $first += $batchSize->size) {
            $last = min($groups, $first + $batchSize->size);
            $values = [];

            foreach ($this->shape->output->definitions() as $name => $_) {
                $values[$name] = [];
            }

            for ($index = $first; $index < $last; $index++) {
                foreach ($this->keys as $name => $keyValues) {
                    $values[$name][] = $keyValues[$index];
                }

                // a combination never seen is null, and a pivot value outside the declared list has no column
                foreach ($this->shape->pivot->values->all() as $column) {
                    $values[$column][] = ($this->accumulators[$index][$column] ?? null)?->value();
                }
            }

            $columns = [];

            foreach ($this->shape->output->definitions() as $name => $definition) {
                $builder = $backend->builder($definition);
                $builder->appendMany($values[$name]);
                $columns[$name] = $builder->finish();
            }

            yield Rows::fromColumns($this->shape->output, $columns, $last - $first);
        }
    }
}
