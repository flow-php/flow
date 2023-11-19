<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\PHP\Type\TypeDetector;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<array<array-key, mixed>, array{name: string, value: array<array-key, mixed>, type: StructureType}>
 */
final class StructureEntry implements \Stringable, Entry
{
    use EntryRef;

    /**
     * @param array<array-key, mixed> $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly array $value,
        private readonly StructureType $type
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (0 === \count($value)) {
            throw InvalidArgumentException::because('Structure must have at least one entry, ' . $name . ' got none.');
        }

        if (!$type->isValid($value)) {
            throw InvalidArgumentException::because('Expected ' . $type->toString() . ' got different types: ' . (new TypeDetector())->detectType($value)->toString());
        }
    }

    public function __serialize() : array
    {
        return [
            'name' => $this->name,
            'value' => $this->value,
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
        $this->value = $data['value'];
        $this->type = $data['type'];
    }

    public function definition() : Definition
    {
        return Definition::structure($this->name, $this->type);
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
        return $this->is($entry->name()) && $entry instanceof self && $this->type->isEqual($entry->type) && (new ArrayComparison())->equals($this->value, $entry->value);
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value), $this->type);
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->value, $this->type);
    }

    public function toString() : string
    {
        return \json_encode($this->value, JSON_THROW_ON_ERROR);
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : array
    {
        return $this->value;
    }
}
