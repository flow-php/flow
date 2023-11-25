<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Schema\Constraint\Any;
use Flow\ETL\Row\Schema\Constraint\VoidConstraint;
use Flow\Serializer\Serializable;

/**
 * @implements Serializable<array{
 *     ref: Reference,
 *     classes:array<class-string<Entry>>,
 *     constraint: Constraint,
 *     metadata: Metadata
 * }>
 */
final class Definition implements Serializable
{
    private Constraint $constraint;

    private Metadata $metadata;

    private readonly Reference $ref;

    /**
     * @param array<class-string<Entry>> $classes
     */
    public function __construct(
        string|Reference $ref,
        private readonly array $classes,
        ?Constraint $constraint = null,
        ?Metadata $metadata = null
    ) {
        if (!\count($classes)) {
            throw new InvalidArgumentException('Schema definition must come with at least one entry class');
        }

        $this->metadata = $metadata ?? Metadata::empty();
        $this->constraint = $constraint ?? new VoidConstraint();
        $this->ref = EntryReference::init($ref);
    }

    public static function array(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [ArrayEntry::class, NullEntry::class] : [ArrayEntry::class], $constraint, $metadata);
    }

    public static function boolean(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [BooleanEntry::class, NullEntry::class] : [BooleanEntry::class], $constraint, $metadata);
    }

    public static function dateTime(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [DateTimeEntry::class, NullEntry::class] : [DateTimeEntry::class], $constraint, $metadata);
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
            ($nullable) ? [EnumEntry::class, NullEntry::class] : [EnumEntry::class],
            $constraint,
            Metadata::with(FlowMetadata::METADATA_ENUM_CLASS, $type)
                ->add(FlowMetadata::METADATA_ENUM_CASES, $type::cases())
                ->merge($metadata ?? Metadata::empty())
        );
    }

    public static function float(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [FloatEntry::class, NullEntry::class] : [FloatEntry::class], $constraint, $metadata);
    }

    public static function integer(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [IntegerEntry::class, NullEntry::class] : [IntegerEntry::class], $constraint, $metadata);
    }

    public static function json(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [JsonEntry::class, NullEntry::class] : [JsonEntry::class], $constraint, $metadata);
    }

    public static function list(string|Reference $entry, ListType $type, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            $nullable ? [ListEntry::class, NullEntry::class] : [ListEntry::class],
            $constraint,
            Metadata::empty()->add(FlowMetadata::METADATA_LIST_ENTRY_TYPE, $type)->merge($metadata ?? Metadata::empty())
        );
    }

    public static function map(string|Reference $entry, MapType $type, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            $nullable ? [Entry\MapEntry::class, NullEntry::class] : [Entry\MapEntry::class],
            $constraint,
            Metadata::empty()->add(FlowMetadata::METADATA_MAP_ENTRY_TYPE, $type)->merge($metadata ?? Metadata::empty())
        );
    }

    public static function null(string|Reference $entry, ?Metadata $metadata = null) : self
    {
        return new self($entry, [NullEntry::class], null, $metadata);
    }

    public static function object(string|Reference $entry, ObjectType $type, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            ($nullable) ? [ObjectEntry::class, NullEntry::class] : [ObjectEntry::class],
            $constraint,
            Metadata::empty()->add(FlowMetadata::METADATA_OBJECT_ENTRY_TYPE, $type)->merge($metadata ?? Metadata::empty())
        );
    }

    public static function string(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [StringEntry::class, NullEntry::class] : [StringEntry::class], $constraint, $metadata);
    }

    public static function structure(string|Reference $entry, StructureType $type, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            $nullable ? [Entry\StructureEntry::class, NullEntry::class] : [Entry\StructureEntry::class],
            $constraint,
            Metadata::empty()->add(FlowMetadata::METADATA_STRUCTURE_ENTRY_TYPE, $type)->merge($metadata ?? Metadata::empty())
        );
    }

    /**
     * @param array<class-string<Entry>> $entryClasses
     *
     * @return Definition
     */
    public static function union(string|Reference $entry, array $entryClasses, ?Constraint $constraint = null, ?Metadata $metadata = null)
    {
        $types = \array_values(\array_unique($entryClasses));

        if (\count($types) <= 1) {
            throw new InvalidArgumentException('Union type requires at least two unique entry types.');
        }

        return new self($entry, $types, $constraint, $metadata);
    }

    public static function uuid(string|Reference $entry, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, [UuidEntry::class], $constraint, $metadata);
    }

    public static function xml(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [XMLEntry::class, NullEntry::class] : [XMLEntry::class], $constraint, $metadata);
    }

    public static function xml_node(string|Reference $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [Entry\XMLNodeEntry::class, NullEntry::class] : [Entry\XMLNodeEntry::class], $constraint, $metadata);
    }

    // @codeCoverageIgnoreStart
    public function __serialize() : array
    {
        return [
            'ref' => $this->ref,
            'classes' => $this->classes,
            'constraint' => $this->constraint,
            'metadata' => $this->metadata,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->ref = $data['ref'];
        $this->classes = $data['classes'];
        $this->constraint = $data['constraint'];
        $this->metadata = $data['metadata'];
    }

    public function constraint() : Constraint
    {
        return $this->constraint;
    }
    // @codeCoverageIgnoreEnd

    public function entry() : Reference
    {
        return $this->ref;
    }

    public function isEqual(self $definition) : bool
    {
        $classes = $this->classes;
        $otherClasses = $definition->classes;

        \sort($classes);
        \sort($otherClasses);

        if ($classes !== $otherClasses) {
            return false;
        }

        if ($this->constraint != $definition->constraint) {
            return false;
        }

        return $this->metadata->isEqual($definition->metadata);
    }

    public function isNullable() : bool
    {
        return \in_array(NullEntry::class, $this->classes, true);
    }

    public function isUnion() : bool
    {
        $types = [];

        foreach ($this->types() as $type) {
            if ($type !== NullEntry::class) {
                $types[] = $type;
            }
        }

        return \count($types) > 1;
    }

    public function matches(Entry $entry) : bool
    {
        if ($this->isNullable() && $entry instanceof Entry\NullEntry && $entry->is($this->ref)) {
            return true;
        }

        if (!$entry->is($this->ref)) {
            return false;
        }

        $isTypeValid = false;

        foreach ($this->classes as $entryClass) {
            if ($entry instanceof $entryClass) {
                $isTypeValid = true;

                break;
            }
        }

        if (!$isTypeValid) {
            return false;
        }

        return $this->constraint->isSatisfiedBy($entry);
    }

    public function merge(self $definition) : self
    {
        $constraint = new Any($this->constraint, $definition->constraint);

        if ($this->constraint instanceof VoidConstraint) {
            $constraint = $definition->constraint;
        }

        if ($definition->constraint instanceof VoidConstraint) {
            $constraint = $this->constraint;
        }

        return new self(
            $this->ref,
            \array_values(\array_unique(\array_merge($this->types(), $definition->types()))),
            $constraint,
            $this->metadata->merge($definition->metadata)
        );
    }

    public function metadata() : Metadata
    {
        return $this->metadata;
    }

    public function nullable() : self
    {
        if (!\in_array(NullEntry::class, $this->classes, true)) {
            return new self($this->ref, \array_merge($this->classes, [NullEntry::class]), $this->constraint, $this->metadata);
        }

        return $this;
    }

    /**
     * @return array<class-string<Entry>>
     */
    public function types() : array
    {
        return $this->classes;
    }
}
