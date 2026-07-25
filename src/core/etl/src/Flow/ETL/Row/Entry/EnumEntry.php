<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition\EnumDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Native\EnumType;
use UnitEnum;

use function Flow\Types\DSL\type_equals;

/**
 * @template-covariant T of \UnitEnum|null
 *
 * @implements Entry<T>
 */
final class EnumEntry implements Entry
{
    use EntryRef;

    /**
     * @var EnumDefinition<\UnitEnum>
     */
    private EnumDefinition $definition;

    /**
     * @param T $value
     */
    public function __construct(
        private readonly string $name,
        private readonly ?UnitEnum $value,
        ?Metadata $metadata = null,
    ) {
        if ('' === $name) {
            throw InvalidArgumentException::because('Entry name cannot be empty');
        }

        $this->definition = self::buildDefinition($this->name, $this->value, $metadata);
    }

    /**
     * @return EnumDefinition<\UnitEnum>
     */
    private static function buildDefinition(string $name, ?UnitEnum $value, ?Metadata $metadata): EnumDefinition
    {
        return new EnumDefinition(
            $name,
            $value === null ? UnitEnum::class : $value::class,
            $value === null,
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

    public function name(): string
    {
        return $this->name;
    }

    /**
     * @return self<T>
     */
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

    /**
     * @return T
     */
    public function value(): ?UnitEnum
    {
        return $this->value;
    }
}
