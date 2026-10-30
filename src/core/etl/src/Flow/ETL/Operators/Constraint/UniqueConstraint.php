<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Constraint;

use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\Reference\References;
use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Kernels\Bucketing\SeenKeys;
use Flow\ETL\Operators\Constraint;
use Flow\ETL\Operators\Transformer\Stateful;
use Flow\ETL\Values\Rows;
use Flow\Types\Type\TypedValueFormatter;

use function array_values;

final class UniqueConstraint implements Constraint, Stateful
{
    /**
     * @var non-empty-list<string|Reference>
     */
    private readonly array $columns;

    private readonly References $reference;

    private readonly SeenKeys $seen;

    public function __construct(string|Reference $column, string|Reference ...$columns)
    {
        $this->columns = [$column, ...array_values($columns)];
        $this->reference = new References(...$this->columns);
        $this->seen = new SeenKeys();
    }

    public function firstViolation(Rows $rows): ?int
    {
        if ($rows->isEmpty()) {
            return null;
        }

        $new = $this->seen->add((new KeyEncoder($this->reference->names()))->of($rows));

        // rows 0..n-1 all bring a new key until the first that does not
        foreach ($new as $position => $row) {
            if ($row !== $position) {
                return $position;
            }
        }

        return count($new) === $rows->count() ? null : count($new);
    }

    public function fresh(): static
    {
        return new self(...$this->columns);
    }

    public function toString(): string
    {
        return sprintf('Unique constraint on [%s]', implode(', ', array_map(
            static fn(Reference $r) => $r->name(),
            $this->reference->all(),
        )));
    }

    public function violation(Rows $rows, int $index): string
    {
        $formatter = new TypedValueFormatter();
        $violations = [];

        foreach ($rows->schema()->keep(...$this->reference->names())->definitions() as $definition) {
            $name = $definition->name();
            $violations[] =
                $name
                . '<'
                . $definition->type()->toString()
                . '> = '
                . $formatter->format($definition->type(), $rows->column($name)->value($index));
        }

        return sprintf('Values: [%s]', implode(', ', $violations));
    }
}
