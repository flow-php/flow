<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Factory;

use Flow\ETL\Row\Entry;

function string_column(string $name, string $value) : Entry
{
    return new Entry\StringEntry($name, $value);
}

function integer_column(string $name, int $value) : Entry
{
    return new Entry\IntegerEntry($name, $value);
}

function boolean_column(string $name, bool $value) : Entry
{
    return new Entry\BooleanEntry($name, $value);
}

function float_column(string $name, float $value) : Entry
{
    return new Entry\FloatEntry($name, $value);
}

function date_column(string $name, string $value) : Entry
{
    return new Entry\DateEntry($name, new \DateTimeImmutable($value));
}

function dateTime_column(string $name, string $value, string $format = \DateTimeImmutable::ATOM) : Entry
{
    return new Entry\DateTimeEntry($name, new \DateTimeImmutable($value), $format);
}

function array_column(string $name, array $data) : Entry
{
    return new Entry\ArrayEntry($name, $data);
}

function json_column(string $name, array $data) : Entry
{
    return new Entry\JsonEntry($name, $data);
}

function json_object_column(string $name, array $data) : Entry
{
    return Entry\JsonEntry::object($name, $data);
}

function object_column(string $name, object $object) : Entry
{
    return new Entry\ObjectEntry($name, $object);
}
