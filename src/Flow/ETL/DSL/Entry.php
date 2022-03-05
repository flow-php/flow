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
     *
     * @return RowEntry\ArrayEntry
     */
    final public static function array(string $name, array $data) : RowEntry
    {
        return new RowEntry\ArrayEntry($name, $data);
    }

    /**
     * @param string $name
     * @param bool $value
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\BooleanEntry
     */
    final public static function boolean(string $name, bool $value) : RowEntry
    {
        return new RowEntry\BooleanEntry($name, $value);
    }

    /**
     * @param string $name
     * @param Entries ...$entries
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\CollectionEntry
     */
    final public static function collection(string $name, Entries ...$entries) : RowEntry
    {
        return new RowEntry\CollectionEntry($name, ...$entries);
    }

    /**
     * @param string $name
     * @param \DateTimeInterface $value
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\DateTimeEntry
     */
    final public static function datetime(string $name, \DateTimeInterface $value) : RowEntry
    {
        return new RowEntry\DateTimeEntry($name, $value);
    }

    /**
     * @param RowEntry ...$entries
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return Entries
     */
    final public static function entries(RowEntry ...$entries) : Entries
    {
        return new Entries(...$entries);
    }

    /**
     * @param string $name
     * @param float $value
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\FloatEntry
     */
    final public static function float(string $name, float $value) : RowEntry
    {
        return new RowEntry\FloatEntry($name, $value);
    }

    /**
     * @param string $name
     * @param int $value
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\IntegerEntry
     */
    final public static function integer(string $name, int $value) : RowEntry
    {
        return new RowEntry\IntegerEntry($name, $value);
    }

    /**
     * @param string $name
     * @param array<mixed> $data
     *
     * @return RowEntry\JsonEntry
     */
    final public static function json(string $name, array $data) : RowEntry
    {
        return new RowEntry\JsonEntry($name, $data);
    }

    /**
     * @param string $name
     * @param array<mixed> $data
     *
     * @return RowEntry\JsonEntry
     */
    final public static function json_object(string $name, array $data) : RowEntry
    {
        return RowEntry\JsonEntry::object($name, $data);
    }

    /**
     * @param string $name
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\NullEntry
     */
    final public static function null(string $name) : RowEntry
    {
        return new RowEntry\NullEntry($name);
    }

    /**
     * @param string $name
     * @param object $object
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\ObjectEntry
     */
    final public static function object(string $name, object $object) : RowEntry
    {
        return new RowEntry\ObjectEntry($name, $object);
    }

    /**
     * @param string $name
     * @param string $value
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\StringEntry
     */
    final public static function string(string $name, string $value) : RowEntry
    {
        return new RowEntry\StringEntry($name, $value);
    }

    /**
     * @param string $name
     * @param string $value
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\StringEntry
     */
    final public static function string_lower(string $name, string $value) : RowEntry
    {
        return RowEntry\StringEntry::lowercase($name, $value);
    }

    /**
     * @param string $name
     * @param string $value
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\StringEntry
     */
    final public static function string_upper(string $name, string $value) : RowEntry
    {
        return RowEntry\StringEntry::uppercase($name, $value);
    }

    /**
     * @param string $name
     * @param RowEntry ...$entries
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException
     *
     * @return RowEntry\StructureEntry
     */
    final public static function structure(string $name, RowEntry ...$entries) : RowEntry
    {
        return new RowEntry\StructureEntry($name, ...$entries);
    }
}
