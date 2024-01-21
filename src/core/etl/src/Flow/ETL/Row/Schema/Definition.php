<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use function Flow\ETL\DSL\type_array;
use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_enum;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_json;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_null;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\type_uuid;
use function Flow\ETL\DSL\type_xml;
use function Flow\ETL\DSL\type_xml_node;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Row\Entry\XMLNodeEntry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Constraint\Any;
use Flow\ETL\Row\Schema\Constraint\VoidConstraint;

final class Definition
{
    private Constraint $constraint;

    private Metadata $metadata;

    private readonly Reference $ref;

    /**
     * @param class-string<Entry> $entryClass
     */
    public function __construct(
        string|Reference $ref,
        private readonly string $entryClass,
        private readonly Type $type,
        ?Constraint $constraint = null,
        ?Metadata $metadata = null
    ) {
        if (!\is_a($this->entryClass, Entry::class, true)) {
            throw new InvalidArgumentException(\sprintf('Entry class "%s" must implement "%s"', $this->entryClass, Entry::class));
        }

        $this->metadata = $metadata ?? Metadata::empty();
        $this->constraint = $constraint ?? new VoidConstraint();
        $this->ref = EntryReference::init($ref);
    }

    public static function array(string|Reference $entry, bool $empty = false, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ArrayEntry::class, type_array($empty, $nullable), $constraint, $metadata);
    }

    public static function boolean(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, BooleanEntry::class, type_boolean($nullable), $constraint, $metadata);
    }

    public static function dateTime(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, DateTimeEntry::class, type_datetime($nullable), $constraint, $metadata);
    }

    /**
     * @param class-string<\UnitEnum> $type
     */
    public static function enum(string|Reference $entry, string $type, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        if (!\enum_exists($type)) {
            throw new InvalidArgumentException("Enum of type \"{$type}\" not found");
        }

        return new self(
            $entry,
            EnumEntry::class,
            type_enum($type, $nullable),
            $constraint,
            $metadata
        );
    }

    public static function float(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, FloatEntry::class, type_float($nullable), $constraint, $metadata);
    }

    public static function integer(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, IntegerEntry::class, type_int($nullable), $constraint, $metadata);
    }

    public static function json(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, JsonEntry::class, type_json($nullable), $constraint, $metadata);
    }

    public static function list(string|Reference $entry, ListType $type, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            ListEntry::class,
            $type,
            $constraint,
            $metadata
        );
    }

    public static function map(string|Reference $entry, MapType $type, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            MapEntry::class,
            $type,
            $constraint,
            $metadata
        );
    }

    public static function null(string|Reference $entry, ?Metadata $metadata = null) : self
    {
        return new self($entry, NullEntry::class, type_null(), null, $metadata);
    }

    public static function object(string|Reference $entry, ObjectType $type, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            ObjectEntry::class,
            $type,
            $constraint,
            $metadata
        );
    }

    public static function string(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, StringEntry::class, type_string($nullable), $constraint, $metadata);
    }

    public static function structure(string|Reference $entry, StructureType $type, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            StructureEntry::class,
            $type,
            $constraint,
            $metadata
        );
    }

    public static function uuid(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, UuidEntry::class, type_uuid($nullable), $constraint, $metadata);
    }

    public static function xml(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, XMLEntry::class, type_xml($nullable), $constraint, $metadata);
    }

    public static function xml_node(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, XMLNodeEntry::class, type_xml_node($nullable), $constraint, $metadata);
    }

    public function constraint() : Constraint
    {
        return $this->constraint;
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

        if ($this->constraint != $definition->constraint) {
            return false;
        }

        return $this->metadata->isEqual($definition->metadata);
    }

    public function isNullable() : bool
    {
        return $this->type->nullable();
    }

    public function matches(Entry $entry) : bool
    {
        if ($this->isNullable() && $entry instanceof NullEntry && $entry->is($this->ref)) {
            return true;
        }

        if (!$entry->is($this->ref)) {
            return false;
        }

        if ($entry::class !== $this->entryClass) {
            return false;
        }

        return $this->constraint->isSatisfiedBy($entry);
    }

    public function merge(self $definition) : self
    {
        if (!$this->ref->is($definition->ref)) {
            throw new RuntimeException(\sprintf('Cannot merge different definitions, %s and %s', $this->ref->name(), $definition->ref->name()));
        }

        $constraint = new Any($this->constraint, $definition->constraint);

        if ($this->constraint instanceof VoidConstraint) {
            $constraint = $definition->constraint;
        }

        if ($definition->constraint instanceof VoidConstraint) {
            $constraint = $this->constraint;
        }

        if ($this->type instanceof ListType && $definition->type instanceof ListType && !$this->type->isEqual($definition->type)) {
            $thisTypeString = $this->type->element()->toString();
            $definitionTypeString = $definition->type->element()->toString();

            if (\in_array($thisTypeString, ['integer', 'float', '?integer', '?float'], true) && \in_array($definitionTypeString, ['integer', 'float', '?integer', '?float'], true)) {
                return new self(
                    $this->ref,
                    $this->entryClass,
                    type_list(type_float($this->type->element()->type()->nullable() || $definition->type->element()->type()->nullable())),
                    $constraint,
                    $this->metadata->merge($definition->metadata)
                );
            }
        }

        if ($this->entryClass === $definition->entryClass && \in_array($this->entryClass, [ListEntry::class, MapEntry::class, StructureEntry::class], true)) {
            if (!$this->type->isEqual($definition->type)) {
                return new self(
                    $this->ref,
                    ArrayEntry::class,
                    type_array(false, $this->isNullable() || $definition->isNullable()),
                    $constraint,
                    $this->metadata->merge($definition->metadata)
                );
            }
        }

        if ($this->entryClass === $definition->entryClass) {
            return new self(
                $this->ref,
                $this->entryClass,
                $this->type()->merge($definition->type()),
                $constraint,
                $this->metadata->merge($definition->metadata)
            );
        }

        $entryClasses = [$this->entryClass, $definition->entryClass];

        if (\in_array(NullEntry::class, $entryClasses, true)) {
            $entryClasses = \array_values(\array_diff($entryClasses, [NullEntry::class]));

            $type = $this->entryClass === NullEntry::class ? $definition->type : $this->type();

            return new self(
                $this->ref,
                $entryClasses[0],
                $type->makeNullable(true),
                $constraint,
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(StringEntry::class, $entryClasses, true)) {
            return new self(
                $this->ref,
                StringEntry::class,
                type_string($this->isNullable() || $definition->isNullable()),
                $constraint,
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(IntegerEntry::class, $entryClasses, true) && \in_array(FloatEntry::class, $entryClasses, true)) {
            return new self(
                $this->ref,
                FloatEntry::class,
                type_float($this->isNullable() || $definition->isNullable()),
                $constraint,
                $this->metadata->merge($definition->metadata)
            );
        }

        if (\in_array(ArrayEntry::class, $entryClasses, true)) {
            return new self(
                $this->ref,
                ArrayEntry::class,
                type_array(false, $this->isNullable() || $definition->isNullable()),
                $constraint,
                $this->metadata->merge($definition->metadata)
            );
        }

        throw new RuntimeException(\sprintf('Cannot merge definitions for entries, "%s (%s)" and "%s (%s)"', $this->ref->name(), $this->type->toString(), $definition->ref->name(), $definition->type->toString()));
    }

    public function metadata() : Metadata
    {
        return $this->metadata;
    }

    public function nullable() : self
    {
        return new self($this->ref, $this->entryClass, $this->type->makeNullable(true), $this->constraint, $this->metadata);
    }

    public function type() : Type
    {
        return $this->type;
    }
}
