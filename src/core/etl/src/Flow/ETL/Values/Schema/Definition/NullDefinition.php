<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema\Definition;

use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definitions;
use Flow\ETL\Values\Schema\Metadata;
use Flow\Types\Type;

use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_null;

/**
 * The bottom of the type lattice: the column's type is not known. Absorbed by merge() in both
 * directions - null ⊔ T = ?T, T ⊔ null = ?T, null ⊔ null = null. Reaches a materialized Schema
 * only when a column is null in every observed row and no type was declared; that is Parquet's
 * UNKNOWN and Iceberg's unknown. A column that holds strings and sometimes null is
 * StringDefinition(nullable: true), never this.
 *
 * @implements Definition<null>
 */
final readonly class NullDefinition implements Definition
{
    private Metadata $metadata;

    private string $name;

    /**
     * @var Type<null>
     */
    private Type $type;

    public function __construct(string $name, ?Metadata $metadata = null)
    {
        $this->name = $name;
        $this->metadata = $metadata ?? Metadata::empty();
        $this->type = type_null();
    }

    /**
     * @param array<array-key, mixed>|bool|float|int|string $value
     */
    public function addMetadata(string $key, int|string|bool|float|array $value): static
    {
        return new self($this->name, $this->metadata->add($key, $value));
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

        return $definition->isNullable() || type_equals($this->type, $definition->type());
    }

    public function isNullable(): bool
    {
        return true;
    }

    public function isSame(Definition $definition): bool
    {
        if (!$definition->isNullable()) {
            return false;
        }

        if (!type_equals($this->type, $definition->type())) {
            return false;
        }

        return $this->metadata->isEqual($definition->metadata());
    }

    public function makeNullable(bool $nullable = true): static
    {
        return new self($this->name, $this->metadata);
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
            'nullable' => true,
            'metadata' => $this->metadata->normalize(),
        ];
    }

    public function rename(string $newName): static
    {
        return new self($newName, $this->metadata);
    }

    public function setMetadata(Metadata $metadata): static
    {
        return new self($this->name, $metadata);
    }

    public function type(): Type
    {
        return $this->type;
    }
}
