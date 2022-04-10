<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Transformer\Condition as TransformerCondition;
use Flow\ETL\Transformer\Condition\RowCondition;
use Symfony\Component\Validator\Constraint;

class Condition
{
    final public static function all(RowCondition ...$conditions) : RowCondition
    {
        return new TransformerCondition\All(...$conditions);
    }

    final public static function any(RowCondition ...$conditions) : RowCondition
    {
        return new TransformerCondition\Any(...$conditions);
    }

    final public static function array_exists(string $entry, string $path) : RowCondition
    {
        return new TransformerCondition\ArrayDotExists($entry, $path);
    }

    final public static function array_value_equals(string $entry, string $path, mixed $value, bool $identical = true) : RowCondition
    {
        return new TransformerCondition\ArrayDotValueEqualsTo($entry, $path, $value, $identical);
    }

    final public static function array_value_greater_or_equal(string $entry, string $path, mixed $value) : RowCondition
    {
        return new TransformerCondition\ArrayDotValueGreaterOrEqualThan($entry, $path, $value);
    }

    final public static function array_value_greater_than(string $entry, string $path, mixed $value) : RowCondition
    {
        return new TransformerCondition\ArrayDotValueGreaterThan($entry, $path, $value);
    }

    final public static function array_value_less_or_equal(string $entry, string $path, mixed $value) : RowCondition
    {
        return new TransformerCondition\ArrayDotValueLessOrEqualThan($entry, $path, $value);
    }

    final public static function array_value_less_than(string $entry, string $path, mixed $value) : RowCondition
    {
        return new TransformerCondition\ArrayDotValueLessThan($entry, $path, $value);
    }

    final public static function equals_to_value(string $entry, mixed $value, bool $identical = true) : RowCondition
    {
        return new TransformerCondition\EntryValueEqualsTo($entry, $value, $identical);
    }

    final public static function exists(string $entry) : RowCondition
    {
        return new TransformerCondition\EntryExists($entry);
    }

    final public static function greater_or_equals_to_value(string $entry, mixed $value) : RowCondition
    {
        return new TransformerCondition\EntryValueGreaterOrEqualThan($entry, $value);
    }

    final public static function greater_than_value(string $entry, mixed $value) : RowCondition
    {
        return new TransformerCondition\EntryValueGreaterThan($entry, $value);
    }

    final public static function is_array(string $entry) : RowCondition
    {
        return new TransformerCondition\EntryInstanceOf($entry, Entry\ArrayEntry::class);
    }

    final public static function is_boolean(string $entry) : RowCondition
    {
        return new TransformerCondition\EntryInstanceOf($entry, Entry\BooleanEntry::class);
    }

    final public static function is_float(string $entry) : RowCondition
    {
        return new TransformerCondition\EntryInstanceOf($entry, Entry\FloatEntry::class);
    }

    final public static function is_integer(string $entry) : RowCondition
    {
        return new TransformerCondition\EntryInstanceOf($entry, Entry\IntegerEntry::class);
    }

    final public static function is_json(string $entry) : RowCondition
    {
        return new TransformerCondition\EntryInstanceOf($entry, Entry\JsonEntry::class);
    }

    final public static function is_not_null(string $entry) : RowCondition
    {
        return new TransformerCondition\EntryNotNull($entry);
    }

    final public static function is_null(string $entry) : RowCondition
    {
        return new TransformerCondition\EntryInstanceOf($entry, Entry\NullEntry::class);
    }

    final public static function is_object(string $entry) : RowCondition
    {
        return new TransformerCondition\EntryInstanceOf($entry, Entry\ObjectEntry::class);
    }

    final public static function is_string(string $entry) : RowCondition
    {
        return new TransformerCondition\EntryInstanceOf($entry, Entry\StringEntry::class);
    }

    final public static function is_valid(string $entry, Constraint ...$constraints) : RowCondition
    {
        if (!\class_exists(\Symfony\Component\Validator\Validation::class)) {
            throw new RuntimeException("Symfony\Component\Validator\Validation class not found, please add symfony/validator dependency to the project first.");
        }

        return new TransformerCondition\ValidValue($entry, new TransformerCondition\ValidValue\SymfonyValidator($constraints));
    }

    final public static function less_or_equals_value(string $entry, mixed $value) : RowCondition
    {
        return new TransformerCondition\EntryValueLessOrEqualThan($entry, $value);
    }

    final public static function less_than_value(string $entry, mixed $value) : RowCondition
    {
        return new TransformerCondition\EntryValueLessThan($entry, $value);
    }

    final public static function none(RowCondition ...$conditions) : RowCondition
    {
        return new TransformerCondition\None(...$conditions);
    }

    final public static function opposite(RowCondition $condition) : RowCondition
    {
        return new TransformerCondition\Opposite($condition);
    }
}
