<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry as RowEntry;

class Entry
{
    /**
     * @param string $name
     * @param array<mixed> $data
     */
    final public static function array(string $name, array $data) : RowEntry
    {
        return new RowEntry\ArrayEntry($name, $data);
    }

    final public static function boolean(string $name, bool $value) : RowEntry
    {
        return new RowEntry\BooleanEntry($name, $value);
    }

    final public static function collection(string $name, Entries ...$entries) : RowEntry
    {
        return new RowEntry\CollectionEntry($name, ...$entries);
    }

    final public static function datetime(string $name, \DateTimeInterface $value) : RowEntry
    {
        return new RowEntry\DateTimeEntry($name, $value);
    }

    final public static function float(string $name, float $value) : RowEntry
    {
        return new RowEntry\FloatEntry($name, $value);
    }

    final public static function integer(string $name, int $value) : RowEntry
    {
        return new RowEntry\IntegerEntry($name, $value);
    }

    /**
     * @param string $name
     * @param array<mixed> $data
     */
    final public static function json(string $name, array $data) : RowEntry
    {
        return new RowEntry\JsonEntry($name, $data);
    }

    /**
     * @param string $name
     * @param array<mixed> $data
     */
    final public static function json_object(string $name, array $data) : RowEntry
    {
        return RowEntry\JsonEntry::object($name, $data);
    }

    final public static function null(string $name) : RowEntry
    {
        return new RowEntry\NullEntry($name);
    }

    final public static function object(string $name, object $object) : RowEntry
    {
        return new RowEntry\ObjectEntry($name, $object);
    }

    final public static function string(string $name, string $value) : RowEntry
    {
        return new RowEntry\StringEntry($name, $value);
    }

    final public static function string_lower(string $name, string $value) : RowEntry
    {
        return RowEntry\StringEntry::lowercase($name, $value);
    }

    final public static function string_upper(string $name, string $value) : RowEntry
    {
        return RowEntry\StringEntry::uppercase($name, $value);
    }

    final public static function structure(string $name, RowEntry ...$entries) : RowEntry
    {
        return new RowEntry\StructureEntry($name, ...$entries);
    }
}
