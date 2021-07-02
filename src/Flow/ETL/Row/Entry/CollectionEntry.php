<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;

/**
 * @psalm-immutable
 */
final class CollectionEntry implements Entry
{
    private string $key;

    private string $name;

    /**
     * @var Entries[]
     */
    private array $entries;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(string $name, Entries ...$entries)
    {
        if (!\strlen($name)) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->key = \mb_strtolower($name);
        $this->name = $name;
        $this->entries = $entries;
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @return array<mixed>
     */
    public function value() : array
    {
        return \array_map(fn (Entries $entries) : array => $entries->toArray(), $this->entries);
    }

    /**
     * @psalm-suppress InvalidArgument
     */
    public function is(string $name) : bool
    {
        return $this->key === \mb_strtolower($name);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function rename(string $name) : Entry
    {
        return new self($name, ...$this->entries);
    }

    /**
     * @psalm-suppress MixedArgument
     *
     * @throws InvalidArgumentException
     */
    public function map(callable $mapper) : Entry
    {
        return new self($this->name, ...$mapper($this->entries));
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayComparison())->equals($this->value(), $entry->value());
    }
}
