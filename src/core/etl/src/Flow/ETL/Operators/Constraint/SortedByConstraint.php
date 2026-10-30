<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Constraint;

use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\UnresolvedReference;
use Flow\ETL\Kernels\Sort\RowOrder;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Operators\Constraint;
use Flow\ETL\Operators\Transformer\Stateful;
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\Rows;
use Flow\Types\Type\TypedValueFormatter;

use function array_map;
use function array_values;
use function implode;
use function var_export;

final class SortedByConstraint implements Constraint, Stateful
{
    /**
     * The last accepted row, one-row batch - the "previous" of the next batch's first row.
     */
    private ?Rows $previous = null;

    /**
     * @var non-empty-list<SortColumn>
     */
    private readonly array $columns;

    /**
     * A reference is expected in ascending order.
     */
    public function __construct(Reference|SortColumn|string $column, Reference|SortColumn|string ...$columns)
    {
        $this->columns = array_map(
            static fn(Reference|SortColumn|string $column): SortColumn => $column instanceof SortColumn
                ? $column
                : UnresolvedReference::init($column)->asc(),
            [$column, ...array_values($columns)],
        );
    }

    public function firstViolation(Rows $rows): ?int
    {
        if ($rows->isEmpty()) {
            return null;
        }

        $order = new RowOrder($this->columns);
        $keys = $order->keys($rows);
        $violation = null;

        if ($this->previous !== null && $order->compare($order->keys($this->previous), 0, $keys, 0) > 0) {
            $violation = 0;
        }

        for ($i = 1, $count = $rows->count(); $violation === null && $i < $count; $i++) {
            if ($order->compare($keys, $i - 1, $keys, $i) > 0) {
                $violation = $i;
            }
        }

        $last = ($violation ?? $rows->count()) - 1;

        if ($last >= 0) {
            $this->previous = $rows->slice($last, 1);
        }

        return $violation;
    }

    public function fresh(): static
    {
        return new self(...$this->columns);
    }

    public function toString(): string
    {
        $columns = [];

        foreach ($this->columns as $column) {
            $columns[] =
                $column->name
                . ' '
                . $column->order->name
                . ($column->nulls === NullsOrder::defaultFor($column->order) ? '' : ' NULLS ' . $column->nulls->name);
        }

        return sprintf('Sorted constraint on [%s]', implode(', ', $columns));
    }

    public function violation(Rows $rows, int $index): string
    {
        $formatter = new TypedValueFormatter();
        $violations = [];

        foreach ($this->columns as $column) {
            $definition = $rows->schema()->get($column->name);
            // @mago-ignore analysis:mixed-assignment
            $previousValue = $this->previous?->column($column->name)->value(0);

            $violations[] = sprintf(
                '%s<%s> expected %s order, current: %s, previous: %s',
                $column->name,
                $definition->type()->toString(),
                $column->order->name,
                $formatter->format($definition->type(), $rows->column($column->name)->value($index)),
                $previousValue === null ? 'null' : var_export($previousValue, true),
            );
        }

        return implode('; ', $violations);
    }
}
