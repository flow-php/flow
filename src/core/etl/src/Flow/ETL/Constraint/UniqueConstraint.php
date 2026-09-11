<?php

declare(strict_types=1);

namespace Flow\ETL\Constraint;

use Flow\ETL\Constraint;
use Flow\ETL\Constraint\UniqueConstraint\InMemoryStorage;
use Flow\ETL\Constraint\UniqueConstraint\Storage;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Schema;
use Flow\Types\Type\TypedValueFormatter;

use function Flow\ETL\DSL\refs;

final class UniqueConstraint implements Constraint
{
    private readonly References $reference;

    private Storage $storage;

    /**
     * Rows arrive batch by batch and Rows::schema() hands out the same instance for a whole batch,
     * so the kept schema is folded once per batch instead of once per row.
     */
    private ?Schema $keptSchema = null;

    private ?Schema $keptSchemaSource = null;

    public function __construct(string|Reference $column, string|Reference ...$columns)
    {
        $this->reference = refs($column, ...$columns);
        $this->storage = new InMemoryStorage();
    }

    public function isSatisfiedBy(Row $row, Schema $schema): bool
    {
        if ($this->keptSchema === null || $this->keptSchemaSource !== $schema) {
            $this->keptSchema = $schema->keep(...$this->reference);
            $this->keptSchemaSource = $schema;
        }

        $key = $row->hash($this->keptSchema);

        if ($this->storage->has($key)) {
            return false;
        }

        $this->storage->set($key);

        return true;
    }

    public function toString(): string
    {
        return sprintf('Unique constraint on [%s]', implode(', ', array_map(
            static fn(Reference $r) => $r->name(),
            $this->reference->all(),
        )));
    }

    public function violation(Row $row, Schema $schema): string
    {
        $formatter = new TypedValueFormatter();
        $violations = [];

        foreach ($schema->keep(...$this->reference)->definitions() as $definition) {
            $name = $definition->entry()->name();
            $violations[] =
                $name
                . '<'
                . $definition->type()->toString()
                . '> = '
                . $formatter->format($definition->type(), $row->get($name));
        }

        return sprintf('Values: [%s]', implode(', ', $violations));
    }

    public function withStorage(Storage $storage): self
    {
        $this->storage = $storage;

        return $this;
    }
}
