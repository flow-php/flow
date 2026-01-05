<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_optional};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

/**
 * @implements Entry<?int>
 */
final class IntegerEntry implements Entry
{
    use EntryRef;

    private IntegerDefinition $definition;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        private readonly ?int $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = new IntegerDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : IntegerDefinition
    {
        return $this->definition;
    }

    public function duplicate() : static
    {
        return new self($this->name, $this->value, $this->definition->metadata());
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
        return $this->is($entry->name()) && $entry instanceof self && type_equals($this->type(), $entry->type()) && $this->value() === $entry->value();
    }

    public function map(callable $mapper) : static
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function name() : string
    {
        return $this->name;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function rename(string $name) : static
    {
        return new self($name, $this->value);
    }

    public function toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return (string) $this->value();
    }

    public function type() : Type
    {
        return $this->definition->type();
    }

    public function value() : ?int
    {
        return $this->value;
    }

    public function withValue(mixed $value) : static
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->definition->metadata());
    }
}
