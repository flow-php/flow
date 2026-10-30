<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema\Definition;

use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definitions;
use Flow\ETL\Values\Schema\Metadata;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\OptionalType;

use function Flow\Types\DSL\type_equals;

/**
 * @template TElement
 *
 * @implements Definition<list<TElement>>
 */
final readonly class ListDefinition implements Definition
{
    private Metadata $metadata;

    private string $name;

    /**
     * @var ListType<TElement>
     */
    private ListType $type;

    /**
     * @param ListType<TElement> $type
     */
    public function __construct(
        string $name,
        ListType $type,
        private bool $nullable = false,
        ?Metadata $metadata = null,
    ) {
        $this->name = $name;
        $this->metadata = $metadata ?? Metadata::empty();
        $this->type = (new TypeProjection($this->name))->list($type);
    }

    /**
     * @param array<array-key, mixed>|bool|float|int|string $value
     */
    public function addMetadata(string $key, int|string|bool|float|array $value): static
    {
        return new self($this->name, $this->type, $this->nullable, $this->metadata->add($key, $value));
    }

    public function name(): string
    {
        return $this->name;
    }

    public function isCompatible(Definition $definition): bool
    {
        if ($this->name !== $definition->name()) {
            return false;
        }

        if (!$this->nullable && $definition->isNullable()) {
            return false;
        }

        if (!$definition instanceof self) {
            return false;
        }

        $thisElement = $this->type->element();
        $thisElementNullable = false;
        $definitionElement = $definition->type->element();
        $definitionElementNullable = false;

        if ($thisElement instanceof OptionalType) {
            $thisElement = $thisElement->base();
            $thisElementNullable = true;
        }

        if ($definitionElement instanceof OptionalType) {
            $definitionElement = $definitionElement->base();
            $definitionElementNullable = true;
        }

        return Definitions::of(
            $this->name . '.element',
            $thisElement,
            $thisElementNullable,
        )->isCompatible(Definitions::of(
            $definition->name . '.element',
            $definitionElement,
            $definitionElementNullable,
        ));
    }

    public function isNullable(): bool
    {
        return $this->nullable;
    }

    public function isSame(Definition $definition): bool
    {
        if ($this->nullable !== $definition->isNullable()) {
            return false;
        }

        if (!type_equals($this->type, $definition->type())) {
            return false;
        }

        return $this->metadata->isEqual($definition->metadata());
    }

    public function makeNullable(bool $nullable = true): static
    {
        return new self($this->name, $this->type, $nullable, $this->metadata);
    }

    public function matches(mixed $value): bool
    {
        return $value === null ? $this->isNullable() : $this->type()->isValid($value);
    }

    public function merge(Definition $definition): Definition
    {
        return Definitions::merged($this, $definition);
    }

    public function metadata(): Metadata
    {
        return $this->metadata;
    }

    /**
     * @return array<string, mixed>
     */
    public function normalize(): array
    {
        return [
            'ref' => $this->name,
            'type' => $this->type->normalize(),
            'nullable' => $this->nullable,
            'metadata' => $this->metadata->normalize(),
        ];
    }

    public function rename(string $newName): static
    {
        return new self($newName, $this->type, $this->nullable, $this->metadata);
    }

    public function setMetadata(Metadata $metadata): static
    {
        return new self($this->name, $this->type, $this->nullable, $metadata);
    }

    /**
     * @return ListType<TElement>
     */
    public function type(): ListType
    {
        return $this->type;
    }
}
