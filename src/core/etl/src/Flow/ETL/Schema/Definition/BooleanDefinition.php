<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_equals;
use function sprintf;

/**
 * @implements Definition<bool>
 */
final readonly class BooleanDefinition implements Definition
{
    private Metadata $metadata;

    private Reference $ref;

    /**
     * @var Type<bool>
     */
    private Type $type;

    public function __construct(
        string|Reference $ref,
        private bool $nullable = false,
        ?Metadata $metadata = null,
    ) {
        $this->ref = UnresolvedReference::init($ref);
        $this->metadata = $metadata ?? Metadata::empty();
        $this->type = type_boolean();
    }

    /**
     * @param array<array-key, mixed>|bool|float|int|string $value
     */
    public function addMetadata(string $key, int|string|bool|float|array $value): static
    {
        return new self($this->ref, $this->nullable, $this->metadata->add($key, $value));
    }

    public function entry(): Reference
    {
        return $this->ref;
    }

    public function isCompatible(Definition $definition): bool
    {
        if (!$this->ref->is($definition->entry())) {
            return false;
        }

        if (!$this->nullable && $definition->isNullable()) {
            return false;
        }

        return type_equals($this->type, $definition->type());
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
        return new self($this->ref, $nullable, $this->metadata);
    }

    public function matches(mixed $value): bool
    {
        return (new ValueMatch())->matches($this, $value);
    }

    public function merge(Definition $definition): Definition
    {
        if (!$this->ref->is($definition->entry())) {
            throw new RuntimeException(sprintf(
                'Cannot merge different definitions, %s and %s',
                $this->ref->name(),
                $definition->entry()->name(),
            ));
        }

        if ($definition instanceof NullDefinition) {
            return $this->makeNullable()->setMetadata($this->metadata->merge($definition->metadata()));
        }

        if ($definition instanceof self) {
            return new self(
                $this->ref,
                $this->nullable || $definition->nullable,
                $this->metadata->merge($definition->metadata),
            );
        }

        if ($definition instanceof StringDefinition) {
            return new StringDefinition(
                $this->ref,
                $this->nullable || $definition->isNullable(),
                $this->metadata->merge($definition->metadata()),
            );
        }

        if ($definition instanceof UnionDefinition && (new UnionMembers())->contains($definition, $this)) {
            return new UnionDefinition(
                $this->ref,
                $definition->type(),
                $this->nullable || $definition->isNullable(),
                $this->metadata->merge($definition->metadata()),
            );
        }

        return (new CommonType())->merge($this, $definition);
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
            'ref' => $this->ref->name(),
            'type' => $this->type->normalize(),
            'nullable' => $this->nullable,
            'metadata' => $this->metadata->normalize(),
        ];
    }

    public function rename(string $newName): static
    {
        return new self($newName, $this->nullable, $this->metadata);
    }

    public function setMetadata(Metadata $metadata): static
    {
        return new self($this->ref, $this->nullable, $metadata);
    }

    public function type(): Type
    {
        return $this->type;
    }
}
