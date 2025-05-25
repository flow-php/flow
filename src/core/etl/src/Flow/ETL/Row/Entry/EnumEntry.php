<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_enum, type_equals};
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;
use Flow\Types\Type\Native\EnumType;
use UnitEnum;

/**
 * @implements Entry<?UnitEnum, UnitEnum>
 */
final class EnumEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    /**
     * @var EnumType<\UnitEnum>
     */
    private readonly EnumType $type;

    public function __construct(
        private readonly string $name,
        private readonly ?\UnitEnum $value,
        ?Metadata $metadata = null,
    ) {
        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_enum($this->value === null ? \UnitEnum::class : $this->value::class);
    }

    public function __toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->name;
    }

    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, $this->value === null, $this->metadata);
    }

    public function duplicate() : self
    {
        return new self($this->name, $this->value, $this->metadata);
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
        return $entry instanceof self && type_equals($this->type, $entry->type) && $this->value === $entry->value;
    }

    public function map(callable $mapper) : self
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : self
    {
        return new self($name, $this->value);
    }

    public function toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->name;
    }

    public function type() : Type
    {
        return $this->type;
    }

    public function value() : ?\UnitEnum
    {
        return $this->value;
    }

    public function withValue(mixed $value) : Entry
    {
        return new self($this->name, $value);
    }
}
