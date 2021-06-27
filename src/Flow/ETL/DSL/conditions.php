<?php

declare(strict_types=1);

namespace Flow\ETL\DSL\Factory;

use Flow\ETL\Row\Entry;
use Flow\ETL\Transformer\Condition;
use Flow\ETL\Transformer\Condition\RowCondition;

function all(RowCondition ...$conditions) : RowCondition
{
    return new Condition\All(...$conditions);
}

function any(RowCondition ...$conditions) : RowCondition
{
    return new Condition\Any(...$conditions);
}

function arrayExists(string $column, string $path) : RowCondition
{
    return new Condition\ArrayDotExists($column, $path);
}

function arrayValueEquals(string $column, string $path, $value, bool $identical = true) : RowCondition
{
    return new Condition\ArrayDotValueEqualsTo($column, $path, $value, $identical);
}

function arrayValueGreaterOrEqual(string $column, string $path, $value) : RowCondition
{
    return new Condition\ArrayDotValueGreaterOrEqualThan($column, $path, $value);
}

function arrayValueGreater(string $column, string $path, $value) : RowCondition
{
    return new Condition\ArrayDotValueGreaterThan($column, $path, $value);
}

function arrayValueLessOrEqual(string $column, string $path, $value) : RowCondition
{
    return new Condition\ArrayDotValueLessOrEqualThan($column, $path, $value);
}

function arrayValueLess(string $column, string $path, $value) : RowCondition
{
    return new Condition\ArrayDotValueLessThan($column, $path, $value);
}

function exists(string $column) : RowCondition
{
    return new Condition\EntryExists($column);
}

function isString(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\StringEntry::class);
}

function isInteger(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\IntegerEntry::class);
}

function isBoolean(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\BooleanEntry::class);
}

function isFloat(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\FloatEntry::class);
}

function isArray(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\ArrayEntry::class);
}

function isJson(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\JsonEntry::class);
}

function isObject(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\ObjectEntry::class);
}

function isNull(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\NullEntry::class);
}

function isNotNull(string $column) : RowCondition
{
    return new Condition\EntryNotNull($column);
}

function valueEquals(string $column, $value, bool $identical = true) : RowCondition
{
    return new Condition\EntryValueEqualsTo($column, $value, $identical);
}

function valueGreaterOrEqual(string $column, $value) : RowCondition
{
    return new Condition\EntryValueGreaterOrEqualThan($column, $value);
}

function valueGreater(string $column, $value) : RowCondition
{
    return new Condition\EntryValueGreaterThan($column, $value);
}

function valueLessOrEqual(string $column, $value) : RowCondition
{
    return new Condition\EntryValueLessOrEqualThan($column, $value);
}

function valueLess(string $column, $value) : RowCondition
{
    return new Condition\EntryValueLessThan($column, $value);
}

function none(RowCondition $conditions) : RowCondition
{
    return new Condition\None(...$conditions);
}

function opposite(RowCondition $condition) : RowCondition
{
    return new Condition\Opposite($condition);
}
