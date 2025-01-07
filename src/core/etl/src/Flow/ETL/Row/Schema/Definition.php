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
use Flow\ETL\PHP\Type\Logical\{ListType, MapType, StructureType};
use Flow\ETL\PHP\Type\{Type, TypeFactory};
use Flow\ETL\Row\Entry\{BooleanEntry,
    DateEntry,
    DateTimeEntry,
    EnumEntry,
    FloatEntry,
    IntegerEntry,
    JsonEntry,
    ListEntry,
    MapEntry,
    StringEntry,
    StructureEntry,
    TimeEntry,
    UuidEntry,
    XMLEntry};
use Flow\ETL\Row\{Entry, EntryReference, Reference};

final readonly class Definition
{
    private Metadata $metadata;

    private Reference $ref;

    /**
     * @param class-string<Entry<mixed, mixed>> $entryClass
     * @param Type<mixed> $type
     */
    public function __construct(
        string|Reference $ref,
        private string $entryClass,
        private Type $type,
        ?Metadata $metadata = null,
    ) {
        if (!\is_a($this->entryClass, Entry::class, true)) {
            throw new InvalidArgumentException(\sprintf('Entry class "%s" must implement "%s"', $this->entryClass, Entry::class));
        }

        $this->metadata = $metadata ?? Metadata::empty();
        $this->ref = EntryReference::init($ref);
    }

    public static function boolean(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, BooleanEntry::class, type_boolean($nullable), $metadata);
    }

    public static function date(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, DateEntry::class, type_date($nullable), $metadata);
    }

    public static function dateTime(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, DateTimeEntry::class, type_datetime($nullable), $metadata);
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
            EnumEntry::class,
            type_enum($type, $nullable),
            $metadata
        );
    }

    public static function float(string|Reference $entry, bool $nullable = false, int $precision = 6, ?Metadata $metadata = null) : self
    {
        return new self($entry, FloatEntry::class, type_float($nullable, $precision), $metadata);
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
            match ($definition['type']['type']) {
                'boolean' => BooleanEntry::class,
                'float' => FloatEntry::class,
                'integer' => IntegerEntry::class,
                'string' => StringEntry::class,
                'datetime' => DateTimeEntry::class,
                'time' => TimeEntry::class,
                'date' => DateEntry::class,
                'enum' => EnumEntry::class,
                'json', 'array' => JsonEntry::class,
                'list' => ListEntry::class,
                'map' => MapEntry::class,
                'structure' => StructureEntry::class,
                'uuid' => UuidEntry::class,
                'xml' => XMLEntry::class,
                'xml_element' => Entry\XMLElementEntry::class,
                default => throw new InvalidArgumentException(\sprintf('Unknown entry type "%s"', \json_encode($definition['type']))),
            },
            TypeFactory::fromArray($definition['type']),
            Metadata::fromArray($definition['metadata'] ?? [])
        );
    }

    public static function integer(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, IntegerEntry::class, type_int($nullable), $metadata);
    }

    public static function json(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, JsonEntry::class, type_json($nullable), $metadata);
    }

    public static function list(string|Reference $entry, ListType $type, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            ListEntry::class,
            $type,
            $metadata
        );
    }

    public static function map(string|Reference $entry, MapType $type, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            MapEntry::class,
            $type,
            $metadata
        );
    }

    public static function string(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, StringEntry::class, type_string($nullable), $metadata);
    }

    public static function structure(string|Reference $entry, StructureType $type, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            StructureEntry::class,
            $type,
            $metadata
        );
    }

    public static function time(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, TimeEntry::class, type_time($nullable), $metadata);
    }

    public static function uuid(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, UuidEntry::class, type_uuid($nullable), $metadata);
    }

    public static function xml(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, XMLEntry::class, type_xml($nullable), $metadata);
    }

    public static function xml_element(string|Reference $entry, bool $nullable = false, ?Metadata $metadata = null) : self
    {
        return new self($entry, Entry\XMLElementEntry::class, type_xml_element($nullable), $metadata);
    }

    public function entry() : Reference
    {
        return $this->ref;
    }

    public function isEqual(self $definition) : bool
    {
        if ($this->entryClass !== $definition->entryClass) {
            return false;
        }

        if ($this->type->isEqual($definition->type) === false) {
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
        return new self($this->ref, $this->entryClass, $this->type->makeNullable($nullable), $this->metadata);
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

        return $entry::class === $this->entryClass;
    }

    public function merge(self $definition) : self
    {
        if (!$this->ref->is($definition->ref)) {
            throw new RuntimeException(\sprintf('Cannot merge different definitions, %s and %s', $this->ref->name(), $definition->ref->name()));
        }

        if ($this->metadata->has(Metadata::FROM_NULL)) {
            return new self(
                $this->ref,
                $definition->entryClass,
                $definition->type()->makeNullable($this->isNullable() || $definition->isNullable()),
                $definition->metadata->remove(Metadata::FROM_NULL)->merge($this->metadata->remove(Metadata::FROM_NULL))
            );
        }

        if ($definition->metadata()->has(Metadata::FROM_NULL)) {
            return new self(
                $this->ref,
                $this->entryClass,
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
                    $this->entryClass,
                    type_list(type_float($this->type->element()->type()->nullable() || $definition->type->element()->type()->nullable())),
                    $this->metadata->merge($definition->metadata)
                );
            }
        }

        if ($this->entryClass === $definition->entryClass && \in_array($this->entryClass, [ListEntry::class, MapEntry::class, StructureEntry::class], true)) {
            if (!$this->type->isEqual($definition->type)) {
                return new self(
                    $this->ref,
                    JsonEntry::class,
                    type_json($this->isNullable() || $definition->isNullable()),
                    $this->metadata->merge($definition->metadata)
                );
            }
        }

        if ($this->entryClass === $definition->entryClass) {
            return new self(
                $this->ref,
                $this->entryClass,
                $this->type()->merge($definition->type()),
                $this->metadata->merge($definition->metadata)
            );
        }

        $entryClasses = [$this->entryClass, $definition->entryClass];

        if (\in_array(StringEntry::class, $entryClasses, true)) {
            return new self(
                $this->ref,
                StringEntry::class,
                type_string($this->isNullable() || $definition->isNullable()),
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(TimeEntry::class, $entryClasses, true) && \in_array(DateEntry::class, $entryClasses, true)) {
            return new self(
                $this->ref,
                DateTimeEntry::class,
                type_datetime($this->isNullable() || $definition->isNullable()),
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(TimeEntry::class, $entryClasses, true) && \in_array(DateTimeEntry::class, $entryClasses, true)) {
            return new self(
                $this->ref,
                DateTimeEntry::class,
                type_datetime($this->isNullable() || $definition->isNullable()),
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(DateEntry::class, $entryClasses, true) && \in_array(DateTimeEntry::class, $entryClasses, true)) {
            return new self(
                $this->ref,
                DateTimeEntry::class,
                type_datetime($this->isNullable() || $definition->isNullable()),
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(IntegerEntry::class, $entryClasses, true) && \in_array(FloatEntry::class, $entryClasses, true)) {
            return new self(
                $this->ref,
                FloatEntry::class,
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
            $this->entryClass,
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
