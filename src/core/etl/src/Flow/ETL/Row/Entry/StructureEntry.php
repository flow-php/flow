<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\TypeFactory;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<array<array-key, mixed>, array{name: string, structure: array<array-key, mixed>, type: StructureType}>
 */
final class StructureEntry implements \Stringable, Entry
{
    use EntryRef;

    /**
     * @param array<array-key, mixed> $structure
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly array $structure,
        private readonly StructureType $type
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (0 === \count($structure)) {
            throw InvalidArgumentException::because('Structure must have at least one entry, ' . $name . ' got none.');
        }

        if (\count(\array_unique(\array_keys($structure))) !== \count($structure)) {
            throw InvalidArgumentException::because('Each entry name in structure must be unique, given: ' . \implode(', ', $structure));
        }
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'structure' => $this->structure,
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
        $this->structure = $data['structure'];
        $this->type = $data['type'];
    }

    public function definition() : Definition
    {
        $factory = new TypeFactory();
        $elements = [];

        foreach ($this->structure as $name => $value) {
            $elements[] = new StructureElement($name, $factory->getType($value));
        }

        return Definition::structure($this->name, $elements);
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
        return $this->is($entry->name()) && $entry instanceof self && (new ArrayComparison())->equals($this->structure, $entry->structure);
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->structure), $this->type);
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->structure, $this->type);
    }

    public function toString() : string
    {
        return \json_encode($this->structure, JSON_THROW_ON_ERROR);
    }

    public function value() : array
    {
        return $this->structure;
    }
}
