<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_is_any;
use function Flow\Types\DSL\type_is_nullable;
use function Flow\Types\DSL\type_optional;
use function sprintf;

/**
 * @template TElement
 *
 * @implements Definition<list<TElement>>
 */
final class ListDefinition implements Definition
{
    private Metadata $metadata;

    private readonly Reference $ref;

    /**
     * @param ListType<TElement> $type
     */
    public function __construct(
        string|Reference $ref,
        private readonly ListType $type,
        private readonly bool $nullable = false,
        ?Metadata $metadata = null,
    ) {
        $this->ref = EntryReference::init($ref);
        $this->metadata = $metadata ?? Metadata::empty();
    }

    /**
     * @param array<array-key, mixed>|bool|float|int|string $value
     */
    public function addMetadata(string $key, int|string|bool|float|array $value): static
    {
        $this->metadata = $this->metadata->add($key, $value);

        return $this;
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

        $thisElementDef = definition_from_type($this->ref->name() . '.element', $thisElement, $thisElementNullable);
        $definitionElementDef = definition_from_type(
            $definition->ref->name() . '.element',
            $definitionElement,
            $definitionElementNullable,
        );

        return $thisElementDef->isCompatible($definitionElementDef);
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
        if ($this->isNullable() && $entry->is($this->ref)) {
            return true;
        }

        if (!$entry->is($this->ref)) {
            return false;
        }

        return $entry->type() instanceof ListType;
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

        $thisFromNull = $this->metadata->has(Metadata::FROM_NULL);
        $defFromNull = $definition->metadata()->has(Metadata::FROM_NULL);

        if ($thisFromNull && $defFromNull) {
            return $this->makeNullable()->setMetadata($this->metadata->merge($definition->metadata()));
        }

        if ($thisFromNull) {
            return $definition
                ->makeNullable()
                ->setMetadata(
                    $definition
                        ->metadata()
                        ->remove(Metadata::FROM_NULL)
                        ->merge($this->metadata->remove(Metadata::FROM_NULL)),
                );
        }

        if ($defFromNull) {
            return $this->makeNullable()->setMetadata(
                $this->metadata
                    ->remove(Metadata::FROM_NULL)
                    ->merge($definition->metadata()->remove(Metadata::FROM_NULL)),
            );
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

            $thisElementType = $this->type->element();
            $definitionElementType = $definition->type->element();

            if (
                type_is_any($thisElementType, IntegerType::class, FloatType::class)
                && type_is_any($definitionElementType, IntegerType::class, FloatType::class)
            ) {
                return new self(
                    $this->ref,
                    new ListType(
                        type_is_nullable($thisElementType) || type_is_nullable($definitionElementType)
                            ? type_optional(type_float())
                            : type_float(),
                    ),
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

        if ($definition instanceof StringDefinition) {
            return new StringDefinition(
                $this->ref,
                $this->nullable || $definition->isNullable(),
                $this->metadata->merge($definition->metadata()),
            );
        }

        throw new RuntimeException(sprintf('Cannot merge %s with %s', self::class, $definition::class));
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
        $this->metadata = $metadata;

        return $this;
    }

    /**
     * @return ListType<TElement>
     */
    public function type(): ListType
    {
        return $this->type;
    }
}
