<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Factory;

use Flow\ETL\Row\Entry;

function stringColumn(string $name, string $value) : Entry
{
    return new Entry\StringEntry($name, $value);
}

function integerColumn(string $name, int $value) : Entry
{
    return new Entry\IntegerEntry($name, $value);
}

function booleanColumn(string $name, bool $value) : Entry
{
    return new Entry\BooleanEntry($name, $value);
}

function floatColumn(string $name, float $value) : Entry
{
    return new Entry\FloatEntry($name, $value);
}

function dateColumn(string $name, \DateTimeImmutable $value) : Entry
{
    return new Entry\DateEntry($name, $value);
}

function dateTimeColumn(string $name, \DateTimeImmutable $value, string $format = \DateTimeImmutable::ATOM) : Entry
{
    return new Entry\DateTimeEntry($name, $value, $format);
}

function arrayColumn(string $name, array $data) : Entry
{
    return new Entry\ArrayEntry($name, $data);
}

function jsonColumn(string $name, array $data) : Entry
{
    return new Entry\JsonEntry($name, $data);
}

function jsonObjectColumn(string $name, array $data) : Entry
{
    return Entry\JsonEntry::object($name, $data);
}

function objectColumn(string $name, object $object) : Entry
{
    return new Entry\ObjectEntry($name, $object);
}
