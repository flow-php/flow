<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use function Flow\Types\DSL\{type_enum, type_equals, type_optional};
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Schema\Definition\EnumDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Native\EnumType;

/**
 * @implements Entry<?\UnitEnum>
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
        /** @var EnumType<\UnitEnum> $type */
        $type = type_enum($this->value === null ? \UnitEnum::class : $this->value::class);
        $this->type = $type;
    }

    public function __toString() : string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->name;
    }

    /**
     * @return EnumDefinition<\UnitEnum>
     */
    public function definition() : EnumDefinition
    {
        /** @var class-string<\UnitEnum>&literal-string $enumClass */
        $enumClass = $this->value === null ? \UnitEnum::class : $this->value::class;

        return new EnumDefinition($this->name, $enumClass, $this->value === null, $this->metadata);
    }

    public function duplicate() : static
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

    public function map(callable $mapper) : static
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function name() : string
    {
        return $this->name;
    }

    public function rename(string $name) : static
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

    /**
     * @return EnumType<\UnitEnum>
     */
    public function type() : EnumType
    {
        return $this->type;
    }

    public function value() : ?\UnitEnum
    {
        return $this->value;
    }

    public function withValue(mixed $value) : static
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->metadata);
    }
}
