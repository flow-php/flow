<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema\Definition;

use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definitions;
use Flow\ETL\Values\Schema\Metadata;
use Flow\Types\Type\Logical\StructureType;

use function array_key_exists;
use function Flow\Types\DSL\type_equals;

/**
 * @template TElement
 *
 * @implements Definition<array<string, TElement>>
 */
final readonly class StructureDefinition implements Definition
{
    private Metadata $metadata;

    private string $name;

    /**
     * @var StructureType<array<array-key, TElement>>
     */
    private StructureType $type;

    /**
     * @param StructureType<array<array-key, TElement>> $type
     */
    public function __construct(
        string $name,
        StructureType $type,
        private bool $nullable = false,
        ?Metadata $metadata = null,
    ) {
        $this->name = $name;
        $this->metadata = $metadata ?? Metadata::empty();
        $this->type = (new TypeProjection($this->name))->structure($type);
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

        $compatibility = new ElementCompatibility();

        $declaredByName = [];

        foreach ($this->type->elements() as $declared) {
            $declaredByName[$declared->name] = $declared;
        }

        $givenByName = [];

        foreach ($definition->type->elements() as $given) {
            $givenByName[$given->name] = $given;
        }

        foreach ($declaredByName as $declared) {
            $given = $givenByName[$declared->name] ?? null;

            if ($declared->optional) {
                if ($given === null) {
                    continue;
                }
            } elseif ($given === null || $given->optional) {
                // A given optional element may be absent, so it cannot satisfy a declared required one.
                return false;
            }

            if (!$compatibility->isCompatible($this->name . '.' . $declared->name, $declared->type, $given->type)) {
                return false;
            }
        }

        foreach ($givenByName as $given) {
            if (!array_key_exists($given->name, $declaredByName)) {
                return false;
            }
        }

        return true;
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
     * @return StructureType<array<array-key, TElement>>
     */
    public function type(): StructureType
    {
        return $this->type;
    }
}
