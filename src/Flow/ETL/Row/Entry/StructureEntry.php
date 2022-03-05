<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;

/**
 * @implements Entry<array<Entry>, array{name: string, entries: array<Entry>}>
 * @psalm-immutable
 */
final class StructureEntry implements Entry
{
    /**
     * @var array<Entry>
     */
    private array $entries;

    private string $name;

    /**
     * @param string $name
     * @param Entry ...$entries
     *
     * @throws InvalidArgumentException
     */
    public function __construct(string $name, Entry ...$entries)
    {
        if (!\strlen($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->name = $name;
        $this->entries = $entries;
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'entries' => $this->entries,
        ];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->entries = $data['entries'];
    }

    public function is(string $name) : bool
    {
        return \mb_strtolower($this->name) === \mb_strtolower($name);
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayComparison())->equals($this->value(), $entry->value());
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, ...$mapper($this->entries));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, ...$this->entries);
    }

    public function toString() : string
    {
        $array = [];

        foreach ($this->entries as $entry) {
            $array[$entry->name()] = $entry->toString();
        }

        return (string) \json_encode($array);
    }

    public function value() : array
    {
        return \array_values($this->entries);
    }
}
