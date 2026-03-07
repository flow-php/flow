<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_optional};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\Definition\UuidDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;
use Flow\Types\Value\Uuid;

/**
 * @implements Entry<?Uuid>
 */
final class UuidEntry implements Entry
{
    use EntryRef;

    private UuidDefinition $definition;

    private ?Uuid $value;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly string $name,
        Uuid|string|null $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (\is_string($value)) {
            $this->value = Uuid::fromString($value);
        } else {
            $this->value = $value;
        }

        $this->definition = new UuidDefinition($this->name, $this->value === null, $metadata ?: Metadata::empty());
    }

    public static function from(string $name, string $value) : self
    {
        return new self($name, Uuid::fromString($value));
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : UuidDefinition
    {
        return $this->definition;
    }

    public function duplicate() : static
    {
        return new self($this->name, $this->value ? new Uuid($this->value->toString()) : null, $this->definition->metadata());
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
        $entryValue = $entry->value();
        $thisValue = $this->value();

        if ($entryValue === null && $thisValue !== null) {
            return false;
        }

        if ($entryValue !== null && $thisValue === null) {
            return false;
        }

        /**
         * @var Uuid $entryValue
         */
        return $this->is($entry->name()) && $entry instanceof self && type_equals($this->type(), $entry->type()) && $this->value?->isEqual($entryValue);
    }

    public function map(callable $mapper) : static
    {
        return new self($this->name, $mapper($this->value));
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
        return new self($name, $this->value, $this->definition->metadata());
    }

    public function toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->toString();
    }

    public function type() : Type
    {
        return $this->definition->type();
    }

    public function value() : ?Uuid
    {
        return $this->value;
    }

    public function withValue(mixed $value) : static
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->definition->metadata());
    }
}
