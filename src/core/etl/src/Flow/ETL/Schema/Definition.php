<?php

declare(strict_types=1);

namespace Flow\ETL\Schema;

use function Flow\ETL\DSL\{is_nullable};
use function Flow\Types\DSL\{type_boolean, type_date, type_datetime, type_enum, type_equals, type_float, type_integer, type_is, type_is_any, type_json, type_list, type_optional, type_string, type_time, type_uuid, type_xml, type_xml_element, types};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\Row\{Entry, EntryReference, Reference};
use Flow\Types\Type;
use Flow\Types\Type\Logical\{ListType, MapType, OptionalType, StructureType};
use Flow\Types\Type\{Native\FloatType, Native\IntegerType, Native\UnionType, TypeFactory};

final class Definition
{
    private Metadata $metadata;

    private readonly Reference $ref;

    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        string|Reference $ref,
        private readonly Type $type,
        private readonly bool $nullable = false,
        ?Metadata $metadata = null,
    ) {
        if ($type instanceof UnionType || $type instanceof OptionalType) {
            throw new InvalidArgumentException('Schema definition can\'t does not accept UnionType or OptionalType');
        }

        $this->metadata = $metadata ?? Metadata::empty();
        $this->ref = EntryReference::init($ref);
    }

    public static function boolean(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_boolean(), $nullable, $metadata);
    }

    public static function date(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_date(), $nullable, $metadata);
    }

    public static function dateTime(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_datetime(), $nullable, $metadata);
    }

    /**
     * @param class-string<\UnitEnum> $type
     */
    public static function enum(string|Reference $entry, string $type, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        if (!\enum_exists($type)) {
            throw new InvalidArgumentException("Enum of type \"{$type}\" not found");
        }

        return new self(
            $entry,
            type_enum($type),
            $nullable,
            $metadata
        );
    }

    public static function float(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_float(), $nullable, $metadata);
    }

    public static function fromArray(array $definition) : self
    {
        if (!\array_key_exists('ref', $definition)) {
            throw new InvalidArgumentException('Schema definition must contain "ref" key');
        }

        if (!\array_key_exists('type', $definition)) {
            throw new InvalidArgumentException('Schema definition must contain "type" key');
        }

        if (!\is_array($definition['type'])) {
            throw new InvalidArgumentException('Schema definition "type" must be an array, got: ' . \json_encode($definition['type']));
        }

        return new self(
            $definition['ref'],
            TypeFactory::fromArray($definition['type']),
            $definition['nullable'] ?? false,
            Metadata::fromArray($definition['metadata'] ?? [])
        );
    }

    public static function integer(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_integer(), $nullable, $metadata);
    }

    public static function json(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_json(), $nullable, $metadata);
    }

    /**
     * @param ListType<mixed> $type
     */
    public static function list(string|Reference $entry, ListType $type, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            $type,
            $nullable,
            $metadata
        );
    }

    /**
     * @param MapType<array-key, mixed> $type
     */
    public static function map(string|Reference $entry, MapType $type, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            $type,
            $nullable,
            $metadata
        );
    }

    public static function string(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_string(), $nullable, $metadata);
    }

    /**
     * @param StructureType<array> $type
     */
    public static function structure(string|Reference $entry, StructureType $type, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, $type, $nullable, $metadata);
    }

    public static function time(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_time(), $nullable, $metadata);
    }

    public static function uuid(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_uuid(), $nullable, $metadata);
    }

    public static function xml(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_xml(), $nullable, $metadata);
    }

    public static function xml_element(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_xml_element(), $nullable, $metadata);
    }

    public function addMetadata(string $key, int|string|bool|float|array $value) : self
    {
        $this->metadata = $this->metadata->add($key, $value);

        return $this;
    }

    public function entry() : Reference
    {
        return $this->ref;
    }

    /**
     * Checks if another type is compatible with this type. Nullability is validated from a schema evolution perspective.
     * This means that when current type is nullable and the other type is not nullable, it is still compatible.
     * When given type is not nullable and current type is nullable, it is not compatible.
     */
    public function isCompatible(self $definition) : bool
    {
        if (!$this->ref->is($definition->ref)) {
            return false;
        }

        if (!$this->nullable && $definition->nullable) {
            return false;
        }

        if ($this->type instanceof ListType && $definition->type instanceof ListType) {
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

            return (new self($this->ref->name() . '.element', $thisElement, $thisElementNullable))
                ->isCompatible(new self($definition->ref->name() . '.element', $definitionElement, $definitionElementNullable));
        }

        if ($this->type instanceof MapType && $definition->type instanceof MapType) {
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

            return (new self($this->ref->name() . '.key', $thisKey, false))
                    ->isCompatible(new self($definition->ref->name() . '.key', $definitionKey, false))
                && (new self($this->ref->name() . '.value', $thisValue, $thisValueNullable))
                    ->isCompatible(new self($definition->ref->name() . '.value', $definitionValue, $definitionValueNullable));
        }

        if ($this->type instanceof StructureType && $definition->type instanceof StructureType) {
            $thisElements = $this->type->elements();
            $definitionElements = $definition->type->elements();

            if (\count($thisElements) !== \count($definitionElements)) {
                return false;
            }

            foreach ($thisElements as $name => $element) {
                if (!\array_key_exists($name, $definitionElements)) {
                    return false;
                }

                $thisElement = $element;
                $thisElementNullable = false;
                $definitionElement = $definitionElements[$name];
                $definitionElementNullable = false;

                if ($thisElement instanceof OptionalType) {
                    $thisElement = $thisElement->base();
                    $thisElementNullable = true;
                }

                if ($definitionElement instanceof OptionalType) {
                    $definitionElement = $definitionElement->base();
                    $definitionElementNullable = true;
                }

                if (!(new self($this->ref->name() . '.' . $name, $thisElement, $thisElementNullable))
                    ->isCompatible(new self($definition->ref->name() . '.' . $name, $definitionElement, $definitionElementNullable))) {
                    return false;
                }
            }

            return true;
        }

        return type_equals($this->type, $definition->type);
    }

    public function isNullable() : bool
    {
        return $this->nullable;
    }

    public function isSame(self $definition) : bool
    {
        if ($this->nullable !== $definition->nullable) {
            return false;
        }

        if (!type_equals($this->type, $definition->type)) {
            return false;
        }

        return $this->metadata->isEqual($definition->metadata);
    }

    public function makeNullable(bool $nullable = true) : self
    {
        return new self($this->ref, $this->type, $nullable, $this->metadata);
    }

    /**
     * @param Entry<mixed, mixed> $entry
     */
    public function matches(Entry $entry) : bool
    {
        if ($this->isNullable() && $entry->is($this->ref)) {
            return true;
        }

        if (!$entry->is($this->ref)) {
            return false;
        }

        return type_equals($this->type, $entry->type());
    }

    public function merge(self $definition) : self
    {
        $thisType = $this->type;
        $definitionType = $definition->type;

        $types = types($thisType, $definitionType);

        if (!$this->ref->is($definition->ref)) {
            throw new RuntimeException(\sprintf('Cannot merge different definitions, %s and %s', $this->ref->name(), $definition->ref->name()));
        }

        if ($this->metadata->has(Metadata::FROM_NULL) && $definition->metadata()->has(Metadata::FROM_NULL)) {
            return new self(
                $this->ref,
                $thisType,
                true,
                $this->metadata->merge($definition->metadata)
            );
        }

        if ($this->metadata->has(Metadata::FROM_NULL)) {
            return new self(
                $this->ref,
                $definitionType,
                true,
                $definition->metadata->remove(Metadata::FROM_NULL)->merge($this->metadata->remove(Metadata::FROM_NULL))
            );
        }

        if ($definition->metadata()->has(Metadata::FROM_NULL)) {
            return new self(
                $this->ref,
                $thisType,
                true,
                $this->metadata->remove(Metadata::FROM_NULL)->merge($definition->metadata->remove(Metadata::FROM_NULL))
            );
        }

        if (type_is($thisType, ListType::class) && type_is($definitionType, ListType::class) && !type_equals($thisType, $definitionType)) {
            /** @var ListType<mixed> $thisType */
            $thisElementType = $thisType->element();
            /** @var ListType<mixed> $definitionType */
            $definitionElementType = $definitionType->element();

            if (type_is_any($thisElementType, IntegerType::class, FloatType::class) && type_is_any($definitionElementType, IntegerType::class, FloatType::class)) {
                return new self(
                    $this->ref,
                    type_list(
                        is_nullable($thisElementType) || is_nullable($definitionElementType)
                            ? type_optional(type_float())
                            : type_float()
                    ),
                    $this->nullable || $definition->nullable,
                    $this->metadata->merge($definition->metadata)
                );
            }
        }

        if ($thisType::class === $definitionType::class && type_is_any($thisType, ListType::class, MapType::class, StructureType::class)) {
            if (!type_equals($thisType, $definitionType)) {
                return new self(
                    $this->ref,
                    type_json(),
                    $this->nullable || $definition->nullable,
                    $this->metadata->merge($definition->metadata)
                );
            }
        }

        if (type_equals($thisType, $definitionType)) {
            return new self(
                $this->ref,
                $thisType,
                $this->nullable || $definition->nullable,
                $this->metadata->merge($definition->metadata)
            );
        }

        if ($types->has(type_string())) {
            return new self(
                $this->ref,
                type_string(),
                $this->nullable || $definition->nullable,
                $this->metadata->merge($definition->metadata)
            );
        }

        if (($types->has(type_time()) && $types->has(type_date())) || ($types->has(type_time()) && $types->has(type_datetime())) || ($types->has(type_date()) && $types->has(type_datetime()))) {
            return new self(
                $this->ref,
                type_datetime(),
                $this->nullable || $definition->nullable,
                $this->metadata->merge($definition->metadata)
            );
        }

        if ($types->has(type_integer()) && $types->has(type_float())) {

            return new self(
                $this->ref,
                type_float(),
                $this->nullable || $definition->nullable,
                $this->metadata->merge($definition->metadata)
            );
        }

        throw new RuntimeException(\sprintf('Cannot merge definitions for entries, "%s (%s)" and "%s (%s)"', $this->ref->name(), $thisType->toString(), $definition->ref->name(), $definitionType->toString()));
    }

    public function metadata() : Metadata
    {
        return $this->metadata;
    }

    public function normalize() : array
    {
        return [
            'ref' => $this->ref->name(),
            'type' => $this->type->normalize(),
            'nullable' => $this->nullable,
            'metadata' => $this->metadata->normalize(),
        ];
    }

    /**
     * @deprecated Use makeNullable() instead
     */
    public function nullable() : self
    {
        return $this->makeNullable();
    }

    public function rename(string $newName) : self
    {
        return new self(
            $newName,
            $this->type,
            $this->nullable,
            $this->metadata
        );
    }

    public function setMetadata(Metadata $metadata) : self
    {
        $this->metadata = $metadata;

        return $this;
    }

    /**
     * @return Type<mixed>
     */
    public function type() : Type
    {
        return $this->type;
    }
}
