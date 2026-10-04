<?php

declare(strict_types=1);

namespace Flow\ETL\Constraint;

use Flow\ETL\Constraint;
use Flow\ETL\Row\NullsOrder;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Sort\RowOrder;
use Flow\Types\Type\TypedValueFormatter;

use function implode;
use function var_export;

final class SortedByConstraint implements Constraint
{
    /**
     * The last accepted row, one-row batch - the "previous" of the next batch's first row.
     */
    private ?Rows $previous = null;

    private readonly References $references;

    public function __construct(Reference $column, Reference ...$columns)
    {
        $this->references = new References($column, ...$columns);
    }

    public function firstViolation(Rows $rows): ?int
    {
        if ($rows->isEmpty()) {
            return null;
        }

        $order = new RowOrder($this->references->all());
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

    public function toString(): string
    {
        $columns = [];

        foreach ($this->references->all() as $reference) {
            $columns[] =
                $reference->name()
                . ' '
                . $reference->sort()->name
                . (
                    $reference->nulls() === NullsOrder::defaultFor($reference->sort())
                        ? ''
                        : ' NULLS ' . $reference->nulls()->name
                );
        }

        return sprintf('Sorted constraint on [%s]', implode(', ', $columns));
    }

    public function violation(Rows $rows, int $index): string
    {
        $formatter = new TypedValueFormatter();
        $violations = [];

        foreach ($this->references->all() as $reference) {
            $definition = $rows->schema()->get($reference);
            // @mago-ignore analysis:mixed-assignment
            $previousValue = $this->previous?->column($reference->base())->value(0);

            $violations[] = sprintf(
                '%s<%s> expected %s order, current: %s, previous: %s',
                $reference->name(),
                $definition->type()->toString(),
                $reference->sort()->name,
                $formatter->format($definition->type(), $rows->column($reference->base())->value($index)),
                $previousValue === null ? 'null' : var_export($previousValue, true),
            );
        }

        return implode('; ', $violations);
    }
}
