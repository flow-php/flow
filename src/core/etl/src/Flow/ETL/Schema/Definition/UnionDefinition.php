<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\EntryTypeResolver;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Native\UnionType;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_equals;
use function sprintf;

/**
 * @implements Definition<mixed>
 */
final readonly class UnionDefinition implements Definition
{
    private Metadata $metadata;

    private Reference $ref;

    /**
     * @var UnionType<mixed, mixed>
     */
    private UnionType $type;

    /**
     * @template TLeft
     * @template TRight
     *
     * @param UnionType<TLeft, TRight> $type
     */
    public function __construct(
        string|Reference $ref,
        UnionType $type,
        private bool $nullable = false,
        ?Metadata $metadata = null,
    ) {
        $this->ref = EntryReference::init($ref);
        $this->metadata = $metadata ?? Metadata::empty();
        $this->type = (new UnionTypeNormalizer())->normalize($type);
    }

    /**
     * @param array<array-key, mixed>|bool|float|int|string $value
     */
    public function addMetadata(string $key, int|string|bool|float|array $value): static
    {
        return new self($this->ref, $this->type, $this->nullable, $this->metadata->add($key, $value));
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

        if (type_equals($this->type, $definition->type())) {
            return true;
        }

        return (new UnionMembers())->contains($this, $definition);
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
        return new self($this->ref, $this->type, $nullable, $this->metadata);
    }

    public function matches(Entry $entry): bool
    {
        if (!$entry->is($this->ref)) {
            return false;
        }

        if ($entry->value() === null) {
            return $this->isNullable();
        }

        return $this->type->isValid($entry->value());
    }

    /**
     * @throws InvalidArgumentException when the value matches no member of the union
     *
     * @return Definition<mixed>
     */
    public function memberFor(mixed $value): Definition
    {
        return definition_from_type(
            $this->ref,
            (new EntryTypeResolver())->fromUnion($this->type, $value, $this->ref->name()),
            $this->nullable,
            $this->metadata,
        );
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
            if (type_equals($this->type, $definition->type)) {
                return new self(
                    $this->ref,
                    $this->type,
                    $this->nullable || $definition->nullable,
                    $this->metadata->merge($definition->metadata),
                );
            }

            return new JsonDefinition(
                $this->ref,
                $this->nullable || $definition->nullable,
                $this->metadata->merge($definition->metadata),
            );
        }

        if ((new UnionMembers())->contains($this, $definition)) {
            return new self(
                $this->ref,
                $this->type,
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
        return new self($newName, $this->type, $this->nullable, $this->metadata);
    }

    public function setMetadata(Metadata $metadata): static
    {
        return new self($this->ref, $this->type, $this->nullable, $metadata);
    }

    /**
     * @return UnionType<mixed, mixed>
     */
    public function type(): UnionType
    {
        return $this->type;
    }

    public function entryClass(): string
    {
        $left = $this->type->types()->first() ?? throw new RuntimeException(sprintf(
            'Union type of "%s" has no member types',
            $this->ref->name(),
        ));

        if ($left instanceof OptionalType) {
            $left = $left->base();
        }

        return definition_from_type($this->ref, $left)->entryClass();
    }
}
