<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Generator;

use function array_chunk;
use function array_filter;
use function array_keys;
use function count;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class PivotedTable
{
    /**
     * @var array<string, array<array-key, AggregatingFunction>> group key => pivot value => accumulator
     */
    private array $accumulators = [];

    /**
     * @var array<string, GroupKey>
     */
    private array $keys = [];

    public function __construct(
        private readonly GroupBy $groupBy,
        private readonly PivotShape $shape,
    ) {}

    public function accumulate(Rows $batch, Backend $backend): void
    {
        $grouped = $this->groupBy->grouped($batch, $this->shape->input);

        if ($grouped === []) {
            return;
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

        /** @var array<string, array<array-key, list<int>>> $indices */
        $indices = [];

        foreach ($grouped as $index => $rows) {
            $this->keys[$index] ??= $this->groupBy->key($batch, $this->shape->input, $rows[0], $index);
            $this->accumulators[$index] ??= [];

            foreach ($rows as $i) {
                if ($pivotValues[$i] === null) {
                    continue;
                }

                /** @var int|string $column */
                $column = $scalars ? $pivotValues[$i] : $pivotType->assert($pivotValues[$i]);
                $indices[$index][$column][] = $i;
            }
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
        foreach (array_chunk($this->keys, $batchSize->size, preserve_keys: true) as $keys) {
            $values = [];

            foreach ($this->shape->output->definitions() as $name => $_) {
                $values[$name] = [];
            }

            foreach ($keys as $index => $key) {
                // @mago-ignore analysis:mixed-assignment
                foreach ($key as $name => $value) {
                    $values[$name][] = $value;
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

            yield Rows::fromColumns($this->shape->output, $columns, count($keys));
        }
    }
}
