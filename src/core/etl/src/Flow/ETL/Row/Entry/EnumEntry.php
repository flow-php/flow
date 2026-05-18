<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\EnumDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Native\EnumType;
use UnitEnum;

use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_optional;

/**
 * @implements Entry<?\UnitEnum>
 */
final class EnumEntry implements Entry
{
    use EntryRef;

    /**
     * @var EnumDefinition<\UnitEnum>
     */
    private EnumDefinition $definition;

    public function __construct(
        private readonly string $name,
        private readonly ?UnitEnum $value,
        ?Metadata $metadata = null,
    ) {
        /** @var class-string<\UnitEnum>&literal-string $enumClass */
        $enumClass = $this->value === null ? UnitEnum::class : $this->value::class;
        $this->definition = new EnumDefinition(
            $this->name,
            $enumClass,
            $this->value === null,
            $metadata ?: Metadata::empty(),
        );
    }

    public function __toString(): string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->name;
    }

    /**
     * @return EnumDefinition<\UnitEnum>
     */
    public function definition(): EnumDefinition
    {
        return $this->definition;
    }

    public function duplicate(): static
    {
        return new self($this->name, $this->value, $this->definition->metadata());
    }

    public function is(string|Reference $name): bool
    {
        if ($name instanceof Reference) {
            return $this->name === $name->name();
        }

        return $this->name === $name;
    }

    public function isEqual(Entry $entry): bool
    {
        return $entry instanceof self && type_equals($this->type(), $entry->type()) && $this->value === $entry->value;
    }

    public function map(callable $mapper): static
    {
        return new self($this->name, $mapper($this->value()));
    }

    public function name(): string
    {
        return $this->name;
    }

    public function rename(string $name): static
    {
        return new self($name, $this->value, $this->definition->metadata());
    }

    public function toString(): string
    {
        if ($this->value === null) {
            return '';
        }

        return $this->value->name;
    }

    /**
     * @return EnumType<\UnitEnum>
     */
    public function type(): EnumType
    {
        return $this->definition->type();
    }

    public function value(): ?UnitEnum
    {
        return $this->value;
    }

    public function withValue(mixed $value): static
    {
        return new self($this->name, type_optional($this->type())->assert($value), $this->definition->metadata());
    }
}
