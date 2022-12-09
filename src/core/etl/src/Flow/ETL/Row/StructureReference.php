<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

final class StructureReference implements Reference
{
    private ?string $alias = null;

    /** @var array<string> */
    private readonly array $entries;

    public function __construct(string $entry, string ...$entries)
    {
        $this->entries = \array_merge([$entry], $entries);
    }

    public function as(string $alias) : Reference
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
        if ($this->alias !== null) {
            return $this->alias;
        }

        return (string) \current($this->entries);
    }

    public function to() : array
    {
        return $this->entries;
    }
}
