<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_equals, type_optional, type_uuid};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;
use Flow\Types\Value\Uuid;

/**
 * @implements Entry<?Uuid>
 */
final class UuidEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    /**
     * @var Type<Uuid>
     */
    private readonly Type $type;

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

        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_uuid();
    }

    public static function from(string $name, string $value) : self
    {
        return new self($name, Uuid::fromString($value));
    }

    public function __toString() : string
    {
        return $this->toString();
    }

    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, $this->value === null, $this->metadata);
    }

    public function duplicate() : self
    {
        return new self($this->name, $this->value ? new Uuid($this->value->toString()) : null, $this->metadata);
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
        return $this->is($entry->name()) && $entry instanceof self && type_equals($this->type, $entry->type) && $this->value?->isEqual($entryValue);
    }

    public function map(callable $mapper) : self
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
    public function rename(string $name) : self
    {
        return new self($name, $this->value);
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
        return $this->type;
    }

    public function value() : ?Uuid
    {
        return $this->value;
    }

    public function withValue(mixed $value) : self
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
