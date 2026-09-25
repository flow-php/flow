<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\ETL\DSL\ref;

/**
 * @implements Definition<mixed>
 */
final readonly class ForeignTypeDefinition implements Definition
{
    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        private string $name,
        private Type $type,
    ) {}

    public function addMetadata(string $key, int|string|bool|float|array $value): static
    {
        return $this;
    }

    public function entry(): Reference
    {
        return ref($this->name);
    }

    public function isCompatible(Definition $definition): bool
    {
        return false;
    }

    public function isNullable(): bool
    {
        return false;
    }

    public function isSame(Definition $definition): bool
    {
        return $definition === $this;
    }

    public function makeNullable(bool $nullable = true): static
    {
        return $this;
    }

    public function matches(mixed $value): bool
    {
        return $this->type->isValid($value);
    }

    public function merge(Definition $definition): Definition
    {
        return $this;
    }

    public function metadata(): Metadata
    {
        return Metadata::empty();
    }

    public function normalize(): array
    {
        return ['ref' => $this->name, 'type' => $this->type->normalize(), 'nullable' => false, 'metadata' => []];
    }

    public function rename(string $newName): static
    {
        return new self($newName, $this->type);
    }

    public function setMetadata(Metadata $metadata): static
    {
        return $this;
    }

    /**
     * @return Type<mixed>
     */
    public function type(): Type
    {
        return $this->type;
    }
}
