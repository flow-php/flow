<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\CollectionEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 * @implements Serializable<array{entry: string, classes:array<class-string<Entry>>, constraint: null|Constraint}>
 */
final class Definition implements Serializable
{
    /**
     * @var array<class-string<Entry>>
     */
    private array $classes;

    private ?Constraint $constraint;

    private string $entry;

    /**
     * @param string $entry
     * @param array<class-string<Entry>> $classes
     * @param ?Constraint $constraint
     */
    public function __construct(string $entry, array $classes, ?Constraint $constraint = null)
    {
        if (!\count($classes)) {
            throw new InvalidArgumentException('Schema definition must come with at least one entry class');
        }

        $this->classes = $classes;
        $this->constraint = $constraint;
        $this->entry = $entry;
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param ?Constraint $constraint
     *
     * @return self
     */
    public static function array(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ($nullable) ? [ArrayEntry::class, NullEntry::class] : [ArrayEntry::class], $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param ?Constraint $constraint
     *
     * @return self
     */
    public static function boolean(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ($nullable) ? [BooleanEntry::class, NullEntry::class] : [BooleanEntry::class], $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param ?Constraint $constraint
     *
     * @return self
     */
    public static function collection(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ($nullable) ? [CollectionEntry::class, NullEntry::class] : [CollectionEntry::class], $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param ?Constraint $constraint
     *
     * @return self
     */
    public static function dateTime(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ($nullable) ? [DateTimeEntry::class, NullEntry::class] : [DateTimeEntry::class], $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param ?Constraint $constraint
     *
     * @return self
     */
    public static function float(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ($nullable) ? [FloatEntry::class, NullEntry::class] : [FloatEntry::class], $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param ?Constraint $constraint
     *
     * @return self
     */
    public static function integer(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ($nullable) ? [IntegerEntry::class, NullEntry::class] : [IntegerEntry::class], $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param ?Constraint $constraint
     *
     * @return self
     */
    public static function json(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ($nullable) ? [JsonEntry::class, NullEntry::class] : [JsonEntry::class], $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     *
     * @return self
     */
    public static function null(string $entry) : self
    {
        return new self($entry, [NullEntry::class]);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param ?Constraint $constraint
     *
     * @return self
     */
    public static function object(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ($nullable) ? [ObjectEntry::class, NullEntry::class] : [ObjectEntry::class], $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param ?Constraint $constraint
     *
     * @return self
     */
    public static function string(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ($nullable) ? [StringEntry::class, NullEntry::class] : [StringEntry::class], $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param ?Constraint $constraint
     *
     * @return self
     */
    public static function structure(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ($nullable) ? [StructureEntry::class, NullEntry::class] : [StructureEntry::class], $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param array<class-string<Entry>> $entryClasses
     * @param ?Constraint $constraint
     *
     * @return Definition
     */
    public static function union(string $entry, array $entryClasses, ?Constraint $constraint = null)
    {
        return new self($entry, $entryClasses, $constraint);
    }

    // @codeCoverageIgnoreStart
    public function __serialize() : array
    {
        return [
            'entry' => $this->entry,
            'classes' => $this->classes,
            'constraint' => $this->constraint,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entry = $data['entry'];
        $this->classes = $data['classes'];
        $this->constraint = $data['constraint'];
    }

    public function entry() : string
    {
        return $this->entry;
    }

    public function isEqualType(self $definition) : bool
    {
        $classes = $this->classes;
        $otherClasses = $definition->classes;

        \sort($classes);
        \sort($otherClasses);

        return $this->classes === $otherClasses;
    }
    // @codeCoverageIgnoreEnd

    public function isNullable() : bool
    {
        return \in_array(NullEntry::class, $this->classes, true);
    }

    /**
     * @param Entry $entry
     *
     * @return bool
     */
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

        if ($this->constraint !== null) {
            /** @psalm-suppress ImpureMethodCall */
            return $this->constraint->isSatisfiedBy($entry);
        }

        return true;
    }

    public function nullable() : self
    {
        if (!\in_array(NullEntry::class, $this->classes, true)) {
            return new self($this->entry, \array_merge($this->classes, [NullEntry::class]), $this->constraint);
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
