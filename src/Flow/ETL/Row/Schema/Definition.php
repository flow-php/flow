<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\CollectionEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\TypedCollection\Type;
use Flow\ETL\Row\Schema\Constraint\Any;
use Flow\ETL\Row\Schema\Constraint\VoidConstraint;
use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 * @implements Serializable<array{
 *     entry: string,
 *     classes:array<class-string<Entry>>,
 *     constraint: Constraint,
 *     metadata: Metadata
 * }>
 */
final class Definition implements Serializable
{
    public const METADATA_ENUM_CASES = 'flow_enum_vales';

    public const METADATA_LIST_ENTRY_TYPE = 'flow_list_entry_type';

    private Constraint $constraint;

    private Metadata $metadata;

    /**
     * @param array<class-string<Entry>> $classes
     */
    public function __construct(
        private readonly string $entry,
        private readonly array $classes,
        ?Constraint $constraint = null,
        ?Metadata $metadata = null
    ) {
        if (!\count($classes)) {
            throw new InvalidArgumentException('Schema definition must come with at least one entry class');
        }

        $this->metadata = $metadata ?? Metadata::empty();
        $this->constraint = $constraint ?? new VoidConstraint();
    }

    /**
     * @psalm-pure
     */
    public static function array(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [ArrayEntry::class, NullEntry::class] : [ArrayEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     */
    public static function boolean(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [BooleanEntry::class, NullEntry::class] : [BooleanEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     */
    public static function collection(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [CollectionEntry::class, NullEntry::class] : [CollectionEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     */
    public static function dateTime(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [DateTimeEntry::class, NullEntry::class] : [DateTimeEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     */
    public static function enum(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [EnumEntry::class, NullEntry::class] : [EnumEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     */
    public static function float(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [FloatEntry::class, NullEntry::class] : [FloatEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     */
    public static function integer(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [IntegerEntry::class, NullEntry::class] : [IntegerEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     */
    public static function json(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [JsonEntry::class, NullEntry::class] : [JsonEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     * @psalm-suppress ImpureMethodCall
     */
    public static function list(string $entry, Type $type, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self(
            $entry,
            ($nullable) ? [ListEntry::class, NullEntry::class] : [ListEntry::class],
            $constraint,
            ($metadata ?? Metadata::empty())->merge(
                Metadata::empty()->add(self::METADATA_LIST_ENTRY_TYPE, $type)
            )
        );
    }

    /**
     * @psalm-pure
     */
    public static function null(string $entry, ?Metadata $metadata = null) : self
    {
        return new self($entry, [NullEntry::class], null, $metadata);
    }

    /**
     * @psalm-pure
     */
    public static function object(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [ObjectEntry::class, NullEntry::class] : [ObjectEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     */
    public static function string(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [StringEntry::class, NullEntry::class] : [StringEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     */
    public static function structure(string $entry, bool $nullable = false, ?Constraint $constraint = null, ?Metadata $metadata = null) : self
    {
        return new self($entry, ($nullable) ? [StructureEntry::class, NullEntry::class] : [StructureEntry::class], $constraint, $metadata);
    }

    /**
     * @psalm-pure
     *
     * @param array<class-string<Entry>> $entryClasses
     *
     * @return Definition
     */
    public static function union(string $entry, array $entryClasses, ?Constraint $constraint = null, ?Metadata $metadata = null)
    {
        $types = \array_values(\array_unique($entryClasses));

        if (\count($types) <= 1) {
            throw new InvalidArgumentException('Union type requires at least two unique entry types.');
        }

        return new self($entry, $types, $constraint, $metadata);
    }

    // @codeCoverageIgnoreStart
    public function __serialize() : array
    {
        return [
            'entry' => $this->entry,
            'classes' => $this->classes,
            'constraint' => $this->constraint,
            'metadata' => $this->metadata,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entry = $data['entry'];
        $this->classes = $data['classes'];
        $this->constraint = $data['constraint'];
        $this->metadata = $data['metadata'];
    }

    public function constraint() : Constraint
    {
        return $this->constraint;
    }
    // @codeCoverageIgnoreEnd

    public function entry() : string
    {
        return $this->entry;
    }

    public function isEqual(self $definition) : bool
    {
        $classes = $this->classes;
        $otherClasses = $definition->classes;

        \sort($classes);
        \sort($otherClasses);

        if ($this->classes !== $otherClasses) {
            return false;
        }

        if ($this->constraint != $definition->constraint) {
            return false;
        }

        /** @psalm-suppress ImpureMethodCall */
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
        if ($this->isNullable() && $entry instanceof Entry\NullEntry && $entry->is($this->entry)) {
            return true;
        }

        if (!$entry->is($this->entry)) {
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

        /** @psalm-suppress ImpureMethodCall */
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

        /** @psalm-suppress ImpureMethodCall */
        return new self(
            $this->entry,
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
            return new self($this->entry, \array_merge($this->classes, [NullEntry::class]), $this->constraint, $this->metadata);
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
