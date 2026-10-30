<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Kernels\Sort\RowOrder;
use Flow\ETL\Values\Rows;

use function array_values;
use function count;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function sort;
use function sprintf;

final readonly class DiscoveredPivotValues implements PivotValues
{
    public function __construct(
        private int $maxValues = 10_000,
    ) {
        if ($this->maxValues < 1) {
            throw new InvalidArgumentException('discover_pivot_values() must allow at least one value, given: '
            . $this->maxValues);
        }
    }

    /**
     * @param iterable<Rows> $batches the frame's rows, not its source's: the pivot column may be produced by an
     *                                earlier step, and an earlier filter decides which values exist at all
     */
    public function resolve(iterable $batches, Reference $pivot): DeclaredPivotValues
    {
        $distinct = [];
        $type = null;

        foreach ($batches as $batch) {
            if ($batch->isEmpty()) {
                continue;
            }

            $column = $batch->column($pivot->base());
            $type ??= $column->type();

            // @mago-ignore analysis:mixed-assignment
            foreach ($column->values() as $value) {
                if ($value === null) {
                    continue;
                }

                $value = type_union(type_string(), type_integer())->assert($value);
                $distinct[$value] = $value;

                if (count($distinct) > $this->maxValues) {
                    throw new InvalidArgumentException(sprintf(
                        'discover_pivot_values() found more than %d distinct values in column "%s". '
                        . 'Raise the limit, or declare them with pivot_values(...).',
                        $this->maxValues,
                        $pivot->name(),
                    ));
                }
            }
        }

        if ($distinct === []) {
            throw new InvalidArgumentException(sprintf(
                'discover_pivot_values() found no values in column "%s", so the pivot has no columns to declare.',
                $pivot->name(),
            ));
        }

        $values = array_values($distinct);
        // sorted so the discovered column order is deterministic across runs, in the order sortBy() uses
        sort($values, RowOrder::flag($type ?? type_string()));

        return new DeclaredPivotValues(...$values);
    }
}
