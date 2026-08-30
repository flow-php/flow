<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\TypeWidener;

use function array_key_exists;
use function Flow\Types\DSL\type_equals;
use function sprintf;

/**
 * @template TElement
 *
 * @implements Definition<array<string, TElement>>
 */
final readonly class StructureDefinition implements Definition
{
    private Metadata $metadata;

    private Reference $ref;

    /**
     * @var StructureType<array<array-key, TElement>>
     */
    private StructureType $type;

    /**
     * @param StructureType<array<array-key, TElement>> $type
     */
    public function __construct(
        string|Reference $ref,
        StructureType $type,
        private bool $nullable = false,
        ?Metadata $metadata = null,
    ) {
        $this->ref = UnresolvedReference::init($ref);
        $this->metadata = $metadata ?? Metadata::empty();
        $this->type = (new TypeProjection())->structure($type);
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

            if (!$compatibility->isCompatible(
                $this->ref->name() . '.' . $declared->name,
                $declared->type,
                $given->type,
            )) {
                return false;
            }
        }

        if ($this->type->allowsExtra()) {
            return true;
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

        return $entry->type() instanceof StructureType && $this->type->isValid($entry->value());
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
                (new TypeWidener())->widenStructures($this->type, $definition->type),
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
        return new self($newName, $this->type, $this->nullable, $this->metadata);
    }

    public function setMetadata(Metadata $metadata): static
    {
        return new self($this->ref, $this->type, $this->nullable, $metadata);
    }

    /**
     * @return StructureType<array<array-key, TElement>>
     */
    public function type(): StructureType
    {
        return $this->type;
    }

    /**
     * @return class-string<Entry\StructureEntry>
     */
    public function entryClass(): string
    {
        return Entry\StructureEntry::class;
    }
}
