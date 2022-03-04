<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Schema;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\CollectionEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\Serializer\Serializable;

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

    // @codeCoverageIgnoreStart
    public static function array(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ArrayEntry::class, $nullable, $constraint);
    }

    public static function boolean(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, BooleanEntry::class, $nullable, $constraint);
    }

    public static function collection(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, CollectionEntry::class, $nullable, $constraint);
    }

    public static function dateTime(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, DateTimeEntry::class, $nullable, $constraint);
    }

    public static function integer(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, IntegerEntry::class, $nullable, $constraint);
    }

    public static function json(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, JsonEntry::class, $nullable, $constraint);
    }

    public static function object(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, ObjectEntry::class, $nullable, $constraint);
    }

    public static function string(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, StringEntry::class, $nullable, $constraint);
    }

    public static function structure(string $entry, bool $nullable = false, ?Constraint $constraint = null) : self
    {
        return new self($entry, StructureEntry::class, $nullable, $constraint);
    }

    /**
     * @return array{entry: string, class:class-string, nullable: boolean, constraint: null|Constraint}
     */
    public function __serialize() : array
    {
        return [
            'entry' => $this->entry,
            'class' => $this->class,
            'nullable' => $this->nullable,
            'constraint' => $this->constraint,
        ];
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param array{entry: string, class:class-string, nullable: boolean, constraint: null|Constraint} $data
     */
    public function __unserialize(array $data) : void
    {
        $this->entry = $data['entry'];
        $this->class = $data['class'];
        $this->nullable = $data['nullable'];
        $this->constraint = $data['constraint'];
    }

    public function entry() : string
    {
        return \mb_strtolower($this->entry);
    }
    // @codeCoverageIgnoreEnd

    public function matches(Entry $entry) : bool
    {
        if ($this->nullable && $entry instanceof Entry\NullEntry && $entry->is($this->entry)) {
            return true;
        }

        if (!$entry instanceof $this->class || !$entry->is($this->entry)) {
            return false;
        }

        if ($this->constraint !== null) {
            return $this->constraint->isSatisfiedBy($entry);
        }

        return true;
    }
}
