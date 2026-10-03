<?php

declare(strict_types=1);

namespace Flow\ETL\Constraint;

use Flow\ETL\Constraint;
use Flow\ETL\Constraint\UniqueConstraint\InMemoryStorage;
use Flow\ETL\Constraint\UniqueConstraint\Storage;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowHashes;
use Flow\Types\Type\TypedValueFormatter;

use function Flow\ETL\DSL\refs;

final class UniqueConstraint implements Constraint
{
    private readonly References $reference;

    private Storage $storage;

    public function __construct(string|Reference $column, string|Reference ...$columns)
    {
        $this->reference = refs($column, ...$columns);
        $this->storage = new InMemoryStorage();
    }

    public function firstViolation(Rows $rows): ?int
    {
        if ($rows->isEmpty()) {
            return null;
        }

        foreach ((new RowHashes())->of($rows->select(...$this->reference->names())) as $i => $key) {
            if ($this->storage->has($key)) {
                return $i;
            }

            $this->storage->set($key);
        }

        return null;
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

        foreach ($rows->schema()->keep(...$this->reference)->definitions() as $definition) {
            $name = $definition->entry()->name();
            $violations[] =
                $name
                . '<'
                . $definition->type()->toString()
                . '> = '
                . $formatter->format($definition->type(), $rows->column($name)->value($index));
        }

        return sprintf('Values: [%s]', implode(', ', $violations));
    }

    public function withStorage(Storage $storage): self
    {
        $this->storage = $storage;

        return $this;
    }
}
