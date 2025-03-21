<?php

declare(strict_types=1);

namespace Flow\ETL\Constraint;

use function Flow\ETL\DSL\refs;
use Flow\ETL\Constraint\UniqueConstraint\{InMemoryStorage, Storage};
use Flow\ETL\Row\{Reference, References};
use Flow\ETL\{Constraint, Row};

final class UniqueConstraint implements Constraint
{
    private readonly References $reference;

    private Storage $storage;

    public function __construct(string|Reference $column, string|Reference ...$columns)
    {
        $this->reference = refs($column, ...$columns);
        $this->storage = new InMemoryStorage();
    }

    public function isSatisfiedBy(Row $row) : bool
    {
        $key = $row->keep(...$this->reference)->hash();

        if ($this->storage->has($key)) {
            return false;
        }

        $this->storage->set($key);

        return true;
    }

    public function toString() : string
    {
        return sprintf(
            'Unique constraint on [%s]',
            implode(', ', array_map(static fn (Reference $r) => $r->name(), $this->reference->all()))
        );
    }

    public function violation(Row $row) : string
    {
        $violations = [];

        foreach ($row->keep(...$this->reference)->entries()->all() as $entry) {
            $violations[] = $entry->name() . '<' . $entry->type()->toString() . '> = ' . $entry->toString();
        }

        return sprintf(
            'Values: [%s]',
            implode(', ', $violations)
        );
    }

    public function withStorage(Storage $storage) : self
    {
        $this->storage = $storage;

        return $this;
    }
}
