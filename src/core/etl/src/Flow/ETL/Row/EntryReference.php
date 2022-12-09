<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

final class EntryReference implements Reference
{
    private ?string $alias = null;

    public function __construct(private readonly string $entry)
    {
    }

    public function as(string $alias) : self
    {
        $this->alias = $alias;

        return $this;
    }

    public function hasAlias() : bool
    {
        return $this->alias !== null;
    }

    public function name() : string
    {
        return $this->alias ?? $this->entry;
    }

    public function to() : string
    {
        return $this->entry;
    }
}
