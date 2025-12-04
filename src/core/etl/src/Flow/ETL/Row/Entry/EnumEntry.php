<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_enum, type_equals, type_optional};
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Type;

/**
 * @implements Entry<?\UnitEnum>
 */
final class EnumEntry implements Entry
{
    use EntryRef;

    private Metadata $metadata;

    /**
     * @var Type<\UnitEnum>
     */
    private readonly Type $type;

    public function __construct(
        private readonly string $name,
        private readonly ?\UnitEnum $value,
        ?Metadata $metadata = null,
    ) {
        $this->metadata = $metadata ?: Metadata::empty();
        $this->type = type_enum($this->value === null ? \UnitEnum::class : $this->value::class);
    }

    #[\Override]
    public function __toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->name;
    }

    #[\Override]
    public function definition() : Definition
    {
        return new Definition($this->name, $this->type, $this->value === null, $this->metadata);
    }

    #[\Override]
    public function duplicate() : self
    {
        return new self($this->name, $this->value, $this->metadata);
    }

    #[\Override]
    public function is(string|Reference $name) : bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    #[\Override]
    public function isEqual(Entry $entry) : bool
    {
        return $entry instanceof self && type_equals($this->type, $entry->type) && $this->value === $entry->value;
    }

    #[\Override]
    public function map(callable $mapper) : self
    {
        return new self($this->name, $mapper($this->value()));
    }

    #[\Override]
    public function name() : string
    {
        return $this->name;
    }

    #[\Override]
    public function rename(string $name) : self
    {
        return new self($name, $this->value);
    }

    #[\Override]
    public function toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->name;
    }

    #[\Override]
    public function type() : Type
    {
        return $this->type;
    }

    #[\Override]
    public function value() : ?\UnitEnum
    {
        return $this->value;
    }

    #[\Override]
    public function withValue(mixed $value) : self
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
