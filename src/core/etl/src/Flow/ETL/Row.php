<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Hash\{Algorithm, NativePHPHash};
use Flow\ETL\Row\{Entries, Entry, Reference, References, Schema};

final class Row
{
    public function __construct(private readonly Entries $entries)
    {
    }

    /**
     * @param Entry<mixed, mixed> ...$entries
     *
     * @throws InvalidArgumentException
     */
    public static function create(Entry ...$entries) : self
    {
        return new self(new Entries(...$entries));
    }

    /**
     * @param Entry<mixed, mixed> ...$entries
     */
    public static function with(Entry ...$entries) : self
    {
        return self::create(...$entries);
    }

    /**
     * @param Entry<mixed, mixed> ...$entries
     *
     * @throws InvalidArgumentException
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
     *
     * @return Entry<mixed, mixed>
     */
    public function get(string|Reference $ref) : Entry
    {
        return $this->entries->get($ref);
    }

    public function has(string|Reference $ref) : bool
    {
        return $this->entries->has($ref);
    }

    public function hash(Algorithm $algorithm = new NativePHPHash()) : string
    {
        $string = '';

        foreach ($this->entries->sort()->all() as $entry) {
            $string .= $entry->name() . $entry->toString();
        }

        return $algorithm->hash($string);
    }

    public function isEqual(self $row) : bool
    {
        return $this->entries->isEqual($row->entries());
    }

    public function keep(string|Reference ...$names) : self
    {
        $entries = [];

        foreach (References::init(...$names) as $ref) {
            $entries[] = $this->entries->get($ref);
        }

        return new self(new Entries(...$entries));
    }

    /**
     * @param callable(Entry<mixed, mixed>) : Entry<mixed, mixed> $mapper
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

    /**
     * @param Entry<mixed, mixed> ...$entries
     */
    public function set(Entry ...$entries) : self
    {
        return new self($this->entries->set(...$entries));
    }

    public function sortEntries() : self
    {
        return new self($this->entries->sort());
    }

    /**
     * @return array<array-key, mixed>
     */
    public function toArray(bool $withKeys = true) : array
    {
        return $this->entries->toArray($withKeys);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function valueOf(string|Reference $name) : mixed
    {
        return $this->get($name)->value();
    }
}
