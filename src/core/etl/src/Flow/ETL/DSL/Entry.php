<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\Entry as RowEntry;
use Flow\ETL\Row\Entry\TypedCollection\ScalarType;

/**
 * @infection-ignore-all
 */
class Entry
{
    /**
     * @param array<mixed> $data
     *
     * @return RowEntry\ArrayEntry
     */
    final public static function array(string $name, array $data) : RowEntry
    {
        return new RowEntry\ArrayEntry($name, $data);
    }

    /**
     * @throws InvalidArgumentException
     */
    final public static function bool(string $name, bool $value) : RowEntry
    {
        return self::boolean($name, $value);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\BooleanEntry
     */
    final public static function boolean(string $name, bool $value) : RowEntry
    {
        return new RowEntry\BooleanEntry($name, $value);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\CollectionEntry
     */
    final public static function collection(string $name, Entries ...$entries) : RowEntry
    {
        return new RowEntry\CollectionEntry($name, ...$entries);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\DateTimeEntry
     */
    final public static function datetime(string $name, \DateTimeInterface $value) : RowEntry
    {
        return new RowEntry\DateTimeEntry($name, $value);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\DateTimeEntry
     */
    final public static function datetime_string(string $name, string $value) : RowEntry
    {
        return new RowEntry\DateTimeEntry($name, new \DateTimeImmutable($value));
    }

    /**
     * @throws InvalidArgumentException
     */
    final public static function entries(RowEntry ...$entries) : Entries
    {
        return new Entries(...$entries);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\EnumEntry
     */
    final public static function enum(string $name, \UnitEnum $enum) : RowEntry
    {
        return new RowEntry\EnumEntry($name, $enum);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\FloatEntry
     */
    final public static function float(string $name, float $value) : RowEntry
    {
        return new RowEntry\FloatEntry($name, $value);
    }

    final public static function int(string $name, int $value) : RowEntry
    {
        return self::integer($name, $value);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\IntegerEntry
     */
    final public static function integer(string $name, int $value) : RowEntry
    {
        return new RowEntry\IntegerEntry($name, $value);
    }

    /**
     * @param array<mixed> $data
     *
     * @return RowEntry\JsonEntry
     */
    final public static function json(string $name, array $data) : RowEntry
    {
        return new RowEntry\JsonEntry($name, $data);
    }

    /**
     * @param array<mixed> $data
     *
     * @throws InvalidArgumentException
     *
     * @return RowEntry\JsonEntry
     */
    final public static function json_object(string $name, array $data) : RowEntry
    {
        return RowEntry\JsonEntry::object($name, $data);
    }

    /**
     * @param string $data
     *
     * @return RowEntry\JsonEntry
     */
    final public static function json_string(string $name, string $data) : RowEntry
    {
        return RowEntry\JsonEntry::fromJsonString($name, $data);
    }

    /**
     * @param array<bool> $value
     *
     * @throws InvalidArgumentException
     *
     * @return RowEntry\ListEntry
     */
    final public static function list_of_boolean(string $name, array $value) : RowEntry
    {
        return new RowEntry\ListEntry($name, ScalarType::boolean, $value);
    }

    /**
     * @param array<\DateTimeInterface> $value
     *
     * @throws InvalidArgumentException
     *
     * @return RowEntry\ListEntry
     */
    final public static function list_of_datetime(string $name, array $value) : RowEntry
    {
        return new RowEntry\ListEntry($name, new RowEntry\TypedCollection\ObjectType(\DateTimeInterface::class), $value);
    }

    /**
     * @param array<float> $value
     *
     * @throws InvalidArgumentException
     *
     * @return RowEntry\ListEntry
     */
    final public static function list_of_float(string $name, array $value) : RowEntry
    {
        return new RowEntry\ListEntry($name, ScalarType::float, $value);
    }

    /**
     * @param array<int> $value
     *
     * @throws InvalidArgumentException
     *
     * @return RowEntry\ListEntry
     */
    final public static function list_of_int(string $name, array $value) : RowEntry
    {
        return new RowEntry\ListEntry($name, ScalarType::integer, $value);
    }

    /**
     * @param array<\DateTimeInterface> $value
     * @param class-string $class
     *
     * @throws InvalidArgumentException
     *
     * @return RowEntry\ListEntry
     */
    final public static function list_of_objects(string $name, string $class, array $value) : RowEntry
    {
        return new RowEntry\ListEntry($name, new RowEntry\TypedCollection\ObjectType($class), $value);
    }

    /**
     * @param array<string> $value
     *
     * @throws InvalidArgumentException
     *
     * @return RowEntry\ListEntry
     */
    final public static function list_of_string(string $name, array $value) : RowEntry
    {
        return new RowEntry\ListEntry($name, ScalarType::string, $value);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\NullEntry
     */
    final public static function null(string $name) : RowEntry
    {
        return new RowEntry\NullEntry($name);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\ObjectEntry
     */
    final public static function object(string $name, object $object) : RowEntry
    {
        return new RowEntry\ObjectEntry($name, $object);
    }

    final public static function str(string $name, string $value) : RowEntry
    {
        return self::string($name, $value);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\StringEntry
     */
    final public static function string(string $name, string $value) : RowEntry
    {
        return new RowEntry\StringEntry($name, $value);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\StringEntry
     */
    final public static function string_lower(string $name, string $value) : RowEntry
    {
        return RowEntry\StringEntry::lowercase($name, $value);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\StringEntry
     */
    final public static function string_upper(string $name, string $value) : RowEntry
    {
        return RowEntry\StringEntry::uppercase($name, $value);
    }

    final public static function struct(string $name, RowEntry ...$entries) : RowEntry
    {
        return self::structure($name, ...$entries);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return RowEntry\StructureEntry
     */
    final public static function structure(string $name, RowEntry ...$entries) : RowEntry
    {
        return new RowEntry\StructureEntry($name, ...$entries);
    }

    /**
     * @return RowEntry\XMLEntry
     */
    final public static function xml(string $name, \DOMDocument|string $data) : RowEntry
    {
        return new RowEntry\XMLEntry($name, $data);
    }

    final public static function xml_node(string $name, \DOMNode $data) : RowEntry
    {
        return new RowEntry\XMLNodeEntry($name, $data);
    }
}
