<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use function Flow\ETL\DSL\{type_boolean,
    type_date,
    type_datetime,
    type_enum,
    type_float,
    type_int,
    type_json,
    type_list,
    type_string,
    type_time,
    type_uuid,
    type_xml,
    type_xml_element};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\PHP\Type\Logical\{DateTimeType, DateType, ListType, MapType, StructureType, TimeType};
use Flow\ETL\PHP\Type\{Native\FloatType, Native\IntegerType, Native\StringType, Type, TypeFactory};
use Flow\ETL\Row\{Entry, EntryReference, Reference};

final readonly class Definition
{
    private Metadata $metadata;

    private Reference $ref;

    /**
     * @param Type<mixed> $type
     */
    public function __construct(
        string|Reference $ref,
        private Type $type,
        ?Metadata $metadata = null,
    ) {

        $this->metadata = $metadata ?? Metadata::empty();
        $this->ref = EntryReference::init($ref);
    }

    public static function boolean(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_boolean($nullable), $metadata);
    }

    public static function date(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_date($nullable), $metadata);
    }

    public static function dateTime(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_datetime($nullable), $metadata);
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
            type_enum($type, $nullable),
            $metadata
        );
    }

    public static function float(string|Reference $entry, bool $nullable = false, int $precision = 6, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_float($nullable, $precision), $metadata);
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
            Metadata::fromArray($definition['metadata'] ?? [])
        );
    }

    public static function integer(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_int($nullable), $metadata);
    }

    public static function json(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_json($nullable), $metadata);
    }

    public static function list(string|Reference $entry, ListType $type, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            $type,
            $metadata
        );
    }

    public static function map(string|Reference $entry, MapType $type, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            $type,
            $metadata
        );
    }

    public static function string(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_string($nullable), $metadata);
    }

    public static function structure(string|Reference $entry, StructureType $type, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            $type,
            $metadata
        );
    }

    public static function time(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_time($nullable), $metadata);
    }

    public static function uuid(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_uuid($nullable), $metadata);
    }

    public static function xml(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_xml($nullable), $metadata);
    }

    public static function xml_element(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, type_xml_element($nullable), $metadata);
    }

    public function entry() : Reference
    {
        return $this->ref;
    }

    public function isEqual(self $definition) : bool
    {
        if ($this->type->isSame($definition->type) === false) {
            return false;
        }

        return $this->metadata->isEqual($definition->metadata);
    }

    public function isNullable() : bool
    {
        return $this->type->nullable();
    }

    public function makeNullable(bool $nullable = true) : self
    {
        return new self($this->ref, $this->type->makeNullable($nullable), $this->metadata);
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

        return $this->type->isEqual($entry->type());
    }

    public function merge(self $definition) : self
    {
        if (!$this->ref->is($definition->ref)) {
            throw new RuntimeException(\sprintf('Cannot merge different definitions, %s and %s', $this->ref->name(), $definition->ref->name()));
        }

        if ($this->metadata->has(Metadata::FROM_NULL)) {
            return new self(
                $this->ref,
                $definition->type()->makeNullable($this->isNullable() || $definition->isNullable()),
                $definition->metadata->remove(Metadata::FROM_NULL)->merge($this->metadata->remove(Metadata::FROM_NULL))
            );
        }

        if ($definition->metadata()->has(Metadata::FROM_NULL)) {
            return new self(
                $this->ref,
                $this->type()->makeNullable($this->isNullable() || $definition->isNullable()),
                $this->metadata->remove(Metadata::FROM_NULL)->merge($definition->metadata->remove(Metadata::FROM_NULL))
            );
        }

        if ($this->type instanceof ListType && $definition->type instanceof ListType && !$this->type->isEqual($definition->type)) {
            $thisTypeString = $this->type->element()->toString();
            $definitionTypeString = $definition->type->element()->toString();

            if (\in_array($thisTypeString, ['integer', 'float', '?integer', '?float'], true) && \in_array($definitionTypeString, ['integer', 'float', '?integer', '?float'], true)) {
                return new self(
                    $this->ref,
                    type_list(type_float($this->type->element()->nullable() || $definition->type->element()->nullable())),
                    $this->metadata->merge($definition->metadata)
                );
            }
        }

        if ($this->type::class === $definition->type::class && \in_array($this->type::class, [ListType::class, MapType::class, StructureType::class], true)) {
            if (!$this->type->isEqual($definition->type)) {
                return new self(
                    $this->ref,
                    type_json($this->isNullable() || $definition->isNullable()),
                    $this->metadata->merge($definition->metadata)
                );
            }
        }

        if ($this->type->isEqual($definition->type)) {
            return new self(
                $this->ref,
                $this->type()->merge($definition->type()),
                $this->metadata->merge($definition->metadata)
            );
        }

        $types = [$this->type::class, $definition->type::class];

        if (\in_array(StringType::class, $types, true)) {
            return new self(
                $this->ref,
                type_string($this->isNullable() || $definition->isNullable()),
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(TimeType::class, $types, true) && \in_array(DateType::class, $types, true)) {
            return new self(
                $this->ref,
                type_datetime($this->isNullable() || $definition->isNullable()),
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(TimeType::class, $types, true) && \in_array(DateTimeType::class, $types, true)) {
            return new self(
                $this->ref,
                type_datetime($this->isNullable() || $definition->isNullable()),
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(DateType::class, $types, true) && \in_array(DateTimeType::class, $types, true)) {
            return new self(
                $this->ref,
                type_datetime($this->isNullable() || $definition->isNullable()),
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(IntegerType::class, $types, true) && \in_array(FloatType::class, $types, true)) {
            return new self(
                $this->ref,
                type_float($this->isNullable() || $definition->isNullable()),
                $this->metadata->merge($definition->metadata)
            );
        }

        throw new RuntimeException(\sprintf('Cannot merge definitions for entries, "%s (%s)" and "%s (%s)"', $this->ref->name(), $this->type->toString(), $definition->ref->name(), $definition->type->toString()));
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
            $this->metadata
        );
    }

    /**
     * @return Type<mixed>
     */
    public function type() : Type
    {
        return $this->type;
    }
}
