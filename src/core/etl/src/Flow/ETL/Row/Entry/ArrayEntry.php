<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\type_array;
use Flow\ArrayComparison\ArrayComparison;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Definition;

/**
 * @implements Entry<array>
 */
final class ArrayEntry implements Entry
{
    use EntryRef;

    private readonly ArrayType $type;

    /**
     * @param array<mixed> $value
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly array $value
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->type = type_array([] === $this->value);
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return Definition::array($this->name, $this->type->nullable());
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
        return $this->is($entry->name()) && $entry instanceof self && $this->type->isEqual($entry->type) && (new ArrayComparison())->equals($this->value, $entry->value());
    }

    public function map(callable $mapper) : Entry
    {
        return new self($this->name, $mapper($this->value));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : Entry
    {
        return new self($name, $this->value);
    }

    public function toString() : string
    {
        return \json_encode($this->value, \JSON_THROW_ON_ERROR);
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
