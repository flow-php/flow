<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 */
final class Row implements Serializable
{
    private Entries $entries;

    public function __construct(Entries $entries)
    {
        $this->entries = $entries;
    }

    /**
     * @psalm-pure
     */
    public static function create(Entry ...$entries) : self
    {
        return new self(new Entries(...$entries));
    }

    /**
     * @return array{entries: Entries}
     */
    public function __serialize() : array
    {
        return ['entries' => $this->entries];
    }

    /**
     * @param array{entries: Entries} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->entries = $data['entries'];
    }

    public function add(Entry ...$entries) : self
    {
        return new self($this->entries->add(...$entries));
    }

    public function entries() : Entries
    {
        return $this->entries;
    }

    /**
     * @param callable(Entry) : bool $callable
     */
    public function filter(callable $callable) : self
    {
        return new self($this->entries->filter($callable));
    }

    /**
     * @throws InvalidArgumentException
     */
    public function get(string $name) : Entry
    {
        return $this->entries->get($name);
    }

    public function isEqual(self $row) : bool
    {
        return $this->entries->isEqual($row->entries());
    }

    /**
     * @param callable(Entry) : Entry $mapper
     */
    public function map(callable $mapper) : self
    {
        return new self(new Entries(...$this->entries->map($mapper)));
    }

    public function merge(self $row, string $prefix = '_') : self
    {
        return new self(
            $this->entries()->merge(
                $row->map(fn (Entry $entry) : Entry => $entry->rename($prefix . $entry->name()))->entries()
            )
        );
    }

    public function remove_entries(string ...$names) : self
    {
        $namesToRemove = [];

        foreach ($names as $name) {
            if ($this->entries->has($name)) {
                $namesToRemove[] = $name;
            }
        }

        return new self($this->entries->remove(...$namesToRemove));
    }

    public function rename_entry(string $currentName, string $newName) : self
    {
        return new self($this->entries->rename($currentName, $newName));
    }

    public function set(Entry ...$entries) : self
    {
        return new self($this->entries->set(...$entries));
    }

    public function sortEntries() : self
    {
        return new self($this->entries->sort());
    }

    /**
     * @return array<string, mixed>
     */
    public function toArray() : array
    {
        return $this->entries->toArray();
    }

    /**
     * @psalm-suppress MissingReturnType
     * @phpstan-ignore-next-line
     *
     * @throws InvalidArgumentException
     */
    public function valueOf(string $name)
    {
        return $this->get($name)->value();
    }
}
