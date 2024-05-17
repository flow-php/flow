<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\ETL\DSL\type_uuid;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\UuidType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\PHP\Value\Uuid;
use Flow\ETL\Row\Schema\Definition;
use Flow\ETL\Row\{Entry, Reference};

/**
 * @implements Entry<?Uuid>
 */
final class UuidEntry implements Entry
{
    use EntryRef;

    private readonly UuidType $type;

    private ?Uuid $value;

    /**
     * @throws InvalidArgumentException
     */
    public function __construct(private readonly string $name, Uuid|string|null $value)
    {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        if (\is_string($value)) {
            $this->value = Uuid::fromString($value);
        } else {
            $this->value = $value;
        }

        $this->type = type_uuid($this->value === null);
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
        return Definition::uuid($this->name);
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

        return $this->is($entry->name()) && $entry instanceof self && $this->type->isEqual($entry->type) && $this->value?->isEqual($entryValue);
    }

    public function map(callable $mapper) : Entry
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
    public function rename(string $name) : Entry
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
}
