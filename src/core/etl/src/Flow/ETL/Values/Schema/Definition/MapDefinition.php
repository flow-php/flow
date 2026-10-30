<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema\Definition;

use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definitions;
use Flow\ETL\Values\Schema\Metadata;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;

use function Flow\Types\DSL\type_equals;

/**
 * @template TKey of array-key
 * @template TValue
 *
 * @implements Definition<array<TKey, TValue>>
 */
final readonly class MapDefinition implements Definition
{
    private Metadata $metadata;

    private string $name;

    /**
     * @var MapType<TKey, TValue>
     */
    private MapType $type;

    /**
     * @param MapType<TKey, TValue> $type
     */
    public function __construct(
        string $name,
        MapType $type,
        private bool $nullable = false,
        ?Metadata $metadata = null,
    ) {
        $this->name = $name;
        $this->metadata = $metadata ?? Metadata::empty();
        $this->type = (new TypeProjection($this->name))->map($type);
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

        $thisKey = $this->type->key();
        $definitionKey = $definition->type->key();

        $thisValue = $this->type->value();
        $thisValueNullable = false;
        $definitionValue = $definition->type->value();
        $definitionValueNullable = false;

        if ($thisValue instanceof OptionalType) {
            $thisValue = $thisValue->base();
            $thisValueNullable = true;
        }

        if ($definitionValue instanceof OptionalType) {
            $definitionValue = $definitionValue->base();
            $definitionValueNullable = true;
        }

        return (
            Definitions::of($this->name . '.key', $thisKey, false)->isCompatible(Definitions::of(
                $definition->name . '.key',
                $definitionKey,
                false,
            ))
            && Definitions::of($this->name . '.value', $thisValue, $thisValueNullable)->isCompatible(Definitions::of(
                $definition->name . '.value',
                $definitionValue,
                $definitionValueNullable,
            ))
        );
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
     * @return MapType<TKey, TValue>
     */
    public function type(): MapType
    {
        return $this->type;
    }
}
