<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<array<Entries>, array{name: string, entries: array<Entries>, type: ArrayType}>
 */
final class CollectionEntry implements \Stringable, Entry
{
    use EntryRef;

    /**
     * @var array<Entries>
     */
    private readonly array $entries;

    private readonly ArrayType $type;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, Entries ...$entries)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->entries = $entries;
        $this->type = new ArrayType(0 === \count($entries));
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'entries' => $this->entries,
            'type' => $this->type,
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
        $this->type = $data['type'];
    }

    public function definition() : Definition
    {
        return Definition::collection($this->name, $this->type->nullable());
    }

    public function is(string|Reference $name) : bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry) : bool
    {
        return $this->is($entry->name()) && $entry instanceof self && $this->type->isEqual($entry->type) && (new ArrayComparison())->equals($this->value(), $entry->value());
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, ...$mapper($this->entries));
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function rename(string $name) : Entry
    {
        return new self($name, ...$this->entries);
    }

    public function toString() : string
    {
        $array = [];

        foreach ($this->entries as $entries) {
            $entriesArray = [];

            foreach ($entries as $entry) {
                $entriesArray[$entry->name()] = $entry->toString();
            }

            $array[] = $entriesArray;
        }

        return \json_encode($array, JSON_THROW_ON_ERROR);
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : array
    {
        return $this->entries;
    }
}
