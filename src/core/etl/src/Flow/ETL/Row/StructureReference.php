<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;

/**
 * @implements Reference<array{entries: array<string>, alias: ?string}>
 */
final class StructureReference implements Reference
{
    private ?string $alias = null;

    /** @var array<string> */
    private readonly array $entries;

    public function __construct(string ...$entries)
    {
        if (!$entries) {
            throw new InvalidArgumentException('StructureReference requires at least one entry');
        }

        $this->entries = $entries;
    }

    public function __serialize() : array
    {
        return [
            'entries' => $this->entries,
            'alias' => $this->alias,
        ];
    }

    public function __toString() : string
    {
        return $this->name();
    }

    public function __unserialize(array $data) : void
    {
        $this->entries = $data['entries'];
        $this->alias = $data['alias'];
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

    public function is(Reference $ref) : bool
    {
        return $this->name() === $ref->name();
    }

    public function name() : string
    {
        return $this->alias ?? (string) \current($this->entries);
    }

    /**
     * @return array<EntryReference>
     */
    public function to() : array
    {
        $refs = [];

        foreach ($this->entries as $entry) {
            $refs[] = EntryReference::init($entry);
        }

        return $refs;
    }
}
