<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\Schema;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{entries: Entries}>
 */
final class Row implements Serializable
{
    public function __construct(private readonly Entries $entries)
    {
    }

    /**
     * @throws InvalidArgumentException
     */
    public static function create(Entry ...$entries) : self
    {
        return new self(new Entries(...$entries));
    }

    public static function with(Entry ...$entries) : self
    {
        return self::create(...$entries);
    }

    public function __serialize() : array
    {
        return ['entries' => $this->entries];
    }

    public function __unserialize(array $data) : void
    {
        $this->entries = $data['entries'];
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return $this
     */
    public function add(Entry ...$entries) : self
    {
        return new self($this->entries->add(...$entries));
    }

    public function entries() : Entries
    {
        return $this->entries;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function get(string|EntryReference $ref) : Entry
    {
        return $this->entries->get($ref);
    }

    public function has(string|EntryReference $ref) : bool
    {
        return $this->entries->has($ref);
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

    /**
     * @throws InvalidArgumentException
     */
    public function merge(self $row, string $prefix = '_') : self
    {
        return new self(
            $this->entries()->merge(
                $row->map(fn (Entry $entry) : Entry => $entry->rename($prefix . $entry->name()))->entries()
            )
        );
    }

    public function remove(string|Reference ...$names) : self
    {
        $namesToRemove = [];

        foreach (References::init(...$names) as $ref) {
            if ($this->entries->has($ref)) {
                $namesToRemove[] = $ref;
            }
        }

        return new self($this->entries->remove(...$namesToRemove));
    }

    public function rename(string $currentName, string $newName) : self
    {
        return new self($this->entries->rename($currentName, $newName));
    }

    public function schema() : Schema
    {
        $definitions = [];

        foreach ($this->entries->all() as $entry) {
            $definitions[] = $entry->definition();
        }

        return new Schema(...$definitions);
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
     * @throws InvalidArgumentException
     *
     * @return mixed
     */
    public function valueOf(string|EntryReference $name)
    {
        return $this->get($name)->value();
    }
}
