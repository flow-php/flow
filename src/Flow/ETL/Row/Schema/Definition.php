<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

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
 * @implements Serializable<array{entry: string, class:class-string, nullable: boolean, constraint: null|Constraint}>
 */
final class Definition implements Serializable
{
    /**
     * @var class-string
     */
    private string $class;

    private ?Constraint $constraint;

    private string $entry;

    private bool $nullable;

    /**
     * @param string $entry
     * @param class-string $class
     * @param bool $nullable
     * @param null|Constraint $constraint
     */
    public function __construct(string $entry, string $class, bool $nullable = false, ?Constraint $constraint = null)
    {
        $this->class = $class;
        $this->nullable = $nullable;
        $this->constraint = $constraint;
        $this->entry = $entry;
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param null|Constraint $constraint
     *
     * @return static
     */
    public static function array(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ArrayEntry::class, $nullable, $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param null|Constraint $constraint
     *
     * @return static
     */
    public static function boolean(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, BooleanEntry::class, $nullable, $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param null|Constraint $constraint
     *
     * @return static
     */
    public static function collection(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, CollectionEntry::class, $nullable, $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param null|Constraint $constraint
     *
     * @return static
     */
    public static function dateTime(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, DateTimeEntry::class, $nullable, $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param null|Constraint $constraint
     *
     * @return static
     */
    public static function float(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, FloatEntry::class, $nullable, $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param null|Constraint $constraint
     *
     * @return static
     */
    public static function integer(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, IntegerEntry::class, $nullable, $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param null|Constraint $constraint
     *
     * @return static
     */
    public static function json(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, JsonEntry::class, $nullable, $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     *
     * @return static
     */
    public static function null(string $entry) : self
    {
        return new self($entry, NullEntry::class, true);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param null|Constraint $constraint
     *
     * @return static
     */
    public static function object(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ObjectEntry::class, $nullable, $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param null|Constraint $constraint
     *
     * @return static
     */
    public static function string(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, StringEntry::class, $nullable, $constraint);
    }

    /**
     * @psalm-pure
     *
     * @param string $entry
     * @param bool $nullable
     * @param null|Constraint $constraint
     *
     * @return static
     */
    public static function structure(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, StructureEntry::class, $nullable, $constraint);
    }

    // @codeCoverageIgnoreStart
    public function __serialize() : array
    {
        return [
            'entry' => $this->entry,
            'class' => $this->class,
            'nullable' => $this->nullable,
            'constraint' => $this->constraint,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entry = $data['entry'];
        $this->class = $data['class'];
        $this->nullable = $data['nullable'];
        $this->constraint = $data['constraint'];
    }

    public function entry() : string
    {
        return $this->entry;
    }
    // @codeCoverageIgnoreEnd

    public function isNullable() : bool
    {
        return $this->nullable;
    }

    /**
     * @param Entry $entry
     *
     * @return bool
     */
    public function matches(Entry $entry) : bool
    {
        if ($this->nullable && $entry instanceof Entry\NullEntry && $entry->is($this->entry)) {
            return true;
        }

        if (!$entry instanceof $this->class || !$entry->is($this->entry)) {
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
        return new self($this->entry, $this->class, true, $this->constraint);
    }
}
