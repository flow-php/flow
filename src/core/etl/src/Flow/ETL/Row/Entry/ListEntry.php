<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @template T
 *
 * @implements Entry<array<T>, array{name: string, type: ListType, value: array<T>}>
 */
final class ListEntry implements Entry, TypedCollection
{
    use EntryRef;

    /**
     * @param array<T> $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly array $value,
        private readonly ListType $type,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (!$type->isValid($value)) {
            throw InvalidArgumentException::because('Expected ' . $type->toString() . ' got different types.');
        }
    }

    public function __serialize() : array
    {
        return ['name' => $this->name, 'type' => $this->type, 'value' => $this->value];
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function __unserialize(array $data) : void
    {
        $this->name = $data['name'];
        $this->type = $data['type'];
        $this->value = $data['value'];
    }

    public function definition() : Definition
    {
        return Definition::list($this->name, $this->type);
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
        return $this->is($entry->name())
            && $entry instanceof self
            && (new ArrayComparison())->equals($this->value, $entry->value())
            && $this->type->isEqual($entry->type);
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value), $this->type);
    }

    public function name() : string
    {
        return $this->name;
    }

    public function phpType() : Type
    {
        return $this->type;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->value, $this->type);
    }

    public function toString() : string
    {
        return \json_encode($this->value(), JSON_THROW_ON_ERROR);
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
