<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;

use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_equals;
use function sprintf;

/**
 * @template TKey of array-key
 * @template TValue
 *
 * @implements Definition<array<TKey, TValue>>
 */
final class MapDefinition implements Definition
{
    private Metadata $metadata;

    private readonly Reference $ref;

    /**
     * @param MapType<TKey, TValue> $type
     */
    public function __construct(
        string|Reference $ref,
        private readonly MapType $type,
        private readonly bool $nullable = false,
        ?Metadata $metadata = null,
    ) {
        $this->ref = EntryReference::init($ref);
        $this->metadata = $metadata ?? Metadata::empty();
    }

    /**
     * @param array<array-key, mixed> $value
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

        $thisKeyDef = definition_from_type($this->ref->name() . '.key', $thisKey, false);
        $definitionKeyDef = definition_from_type($definition->ref->name() . '.key', $definitionKey, false);

        $thisValueDef = definition_from_type($this->ref->name() . '.value', $thisValue, $thisValueNullable);
        $definitionValueDef = definition_from_type(
            $definition->ref->name() . '.value',
            $definitionValue,
            $definitionValueNullable,
        );

        return $thisKeyDef->isCompatible($definitionKeyDef) && $thisValueDef->isCompatible($definitionValueDef);
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

        return $entry->type() instanceof MapType;
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
     * @return MapType<TKey, TValue>
     */
    public function type(): MapType
    {
        return $this->type;
    }
}
