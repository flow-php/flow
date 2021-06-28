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

function array_exists(string $column, string $path) : RowCondition
{
    return new Condition\ArrayDotExists($column, $path);
}

function array_value_equals(string $column, string $path, $value, bool $identical = true) : RowCondition
{
    return new Condition\ArrayDotValueEqualsTo($column, $path, $value, $identical);
}

function array_value_greaterOrEqual(string $column, string $path, $value) : RowCondition
{
    return new Condition\ArrayDotValueGreaterOrEqualThan($column, $path, $value);
}

function array_value_greater(string $column, string $path, $value) : RowCondition
{
    return new Condition\ArrayDotValueGreaterThan($column, $path, $value);
}

function array_value_less_or_equal(string $column, string $path, $value) : RowCondition
{
    return new Condition\ArrayDotValueLessOrEqualThan($column, $path, $value);
}

function array_valueLess(string $column, string $path, $value) : RowCondition
{
    return new Condition\ArrayDotValueLessThan($column, $path, $value);
}

function exists(string $column) : RowCondition
{
    return new Condition\EntryExists($column);
}

function is_string(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\StringEntry::class);
}

function is_integer(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\IntegerEntry::class);
}

function is_boolean(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\BooleanEntry::class);
}

function is_float(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\FloatEntry::class);
}

function is_array(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\ArrayEntry::class);
}

function is_json(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\JsonEntry::class);
}

function is_object(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\ObjectEntry::class);
}

function is_null(string $column) : RowCondition
{
    return new Condition\EntryInstanceOf($column, Entry\NullEntry::class);
}

function is_not_null(string $column) : RowCondition
{
    return new Condition\EntryNotNull($column);
}

function value_equals(string $column, $value, bool $identical = true) : RowCondition
{
    return new Condition\EntryValueEqualsTo($column, $value, $identical);
}

function value_greater_or_equal(string $column, $value) : RowCondition
{
    return new Condition\EntryValueGreaterOrEqualThan($column, $value);
}

function value_greater(string $column, $value) : RowCondition
{
    return new Condition\EntryValueGreaterThan($column, $value);
}

function value_less_or_equal(string $column, $value) : RowCondition
{
    return new Condition\EntryValueLessOrEqualThan($column, $value);
}

function value_less(string $column, $value) : RowCondition
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
