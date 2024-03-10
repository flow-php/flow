<?php

declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entries, Entry, Reference, References, Schema};

final class Row
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
    public function get(string|Reference $ref) : Entry
    {
        return $this->entries->get($ref);
    }

    public function has(string|Reference $ref) : bool
    {
        return $this->entries->has($ref);
    }

    public function hash(string $algorithm = 'xxh128', bool $binary = false, array $options = []) : string
    {
        if (!\in_array($algorithm, \hash_algos(), true)) {
            throw new \InvalidArgumentException(\sprintf('Hashing algorithm "%s" is not supported', $algorithm));
        }

        $string = '';

        foreach ($this->entries->sort()->all() as $entry) {
            $string .= $entry->name() . $entry->toString();
        }

        return \hash($algorithm, $string, $binary, $options);
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
     * @return array<array-key, mixed>
     */
    public function toArray(bool $withKeys = true) : array
    {
        return $this->entries->toArray($withKeys);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return mixed
     */
    public function valueOf(string|Reference $name)
    {
        return $this->get($name)->value();
    }
}
