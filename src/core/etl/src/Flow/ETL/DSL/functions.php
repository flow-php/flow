<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\Expression\Literal;
use Flow\ETL\Row\StructureReference;

function col(string $entry, string ...$entries) : Reference
{
    if (\count($entries)) {
        return new StructureReference($entry, ...$entries);
    }

    return new EntryReference($entry);
}

function entry(string $entry) : EntryReference
{
    return new EntryReference($entry);
}

/**
 * Alias for entry function.
 */
function ref(string $entry) : EntryReference
{
    return entry($entry);
}

function struct(string ...$entries) : StructureReference
{
    if (!\count($entries)) {
        throw new InvalidArgumentException('struct (StructureReference) require at least one entry');
    }

    $entry = \array_shift($entries);

    return new StructureReference($entry, ...$entries);
}

function lit(mixed $value) : Expression
{
    return new Literal($value);
}

function when(Expression $ref, Expression $then, Expression $else = null) : Expression
{
    return new Expression\When($ref, $then, $else);
}

function array_get(Expression $ref, string $path) : Expression
{
    return new Expression\ArrayGet($ref, $path);
}

function array_get_collection(Expression $ref, string ...$keys) : Expression
{
    return new Expression\ArrayGetCollection($ref, $keys);
}

function array_get_collection_first(Expression $ref, string ...$keys) : Expression
{
    return Expression\ArrayGetCollection::fromFirst($ref, $keys);
}

function array_exists(Expression $ref, string $path) : Expression
{
    return new Expression\ArrayExists($ref, $path);
}

function array_merge(Expression $left, Expression $right) : Expression
{
    return new Expression\ArrayMerge($left, $right);
}

function array_merge_collection(Expression $ref) : Expression
{
    return new Expression\ArrayMergeCollection($ref);
}

function array_key_rename(Expression $ref, string $path, string $newName) : Expression
{
    return new Expression\ArrayKeyRename($ref, $path, $newName);
}

function now(\DateTimeZone $time_zone = new \DateTimeZone('UTC')) : Expression
{
    return new Expression\Now($time_zone);
}

function to_date_time(Expression $ref, string $format = 'Y-m-d H:i:s', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : Expression
{
    return new Expression\ToDateTime($ref, $format, $timeZone);
}

function to_date(Expression $ref, string $format = 'Y-m-d', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : Expression
{
    return new Expression\ToDate($ref, $format, $timeZone);
}

function date_time_format(Expression $ref, string $format) : Expression
{
    return new Expression\DateTimeFormat($ref, $format);
}

/**
 * @param non-empty-string $separator
 */
function split(Expression $ref, string $separator, int $limit = PHP_INT_MAX) : Expression
{
    return new Expression\Split($ref, $separator, $limit);
}

function combine(Expression $keys, Expression $values) : Expression
{
    return new Expression\Combine($keys, $values);
}

function concat(Expression ...$expressions) : Expression
{
    return new Expression\Concat(...$expressions);
}

function hash(Expression $expression, string $algorithm = 'xxh128', bool $binary = false, array $options = []) : Expression
{
    return new Expression\Hash($expression, $algorithm, $binary, $options);
}

function cast(Expression $expression, string $type) : Expression
{
    return new Expression\Cast($expression, $type);
}

function count(Expression $expression) : Expression
{
    return new Expression\Count($expression);
}

/**
 * Unpacks each element of an array into a new entry, using the array key as the entry name.
 *
 * Before:
 * +--+-------------------+
 * |id|              array|
 * +--+-------------------+
 * | 1|{"a":1,"b":2,"c":3}|
 * | 2|{"d":4,"e":5,"f":6}|
 * +--+-------------------+
 *
 * After:
 * +--+-----+-----+-----+-----+-----+
 * |id|arr.b|arr.c|arr.d|arr.e|arr.f|
 * +--+-----+-----+-----+-----+-----+
 * | 1|    2|    3|     |     |     |
 * | 2|     |     |    4|    5|    6|
 * +--+-----+-----+-----+-----+-----+
 */
function array_unpack(Expression $expression, array $skip_keys = [], ?string $entry_prefix = null) : Expression
{
    return new Expression\ArrayUnpack($expression, $skip_keys, $entry_prefix);
}

/**
 * Expands each value into entry, if there are more than one value, multiple rows will be created.
 * Array keys are ignored, only values are used to create new rows.
 *
 * Before:
 *   +--+-------------------+
 *   |id|              array|
 *   +--+-------------------+
 *   | 1|{"a":1,"b":2,"c":3}|
 *   +--+-------------------+
 *
 * After:
 *   +--+--------+
 *   |id|expanded|
 *   +--+--------+
 *   | 1|       1|
 *   | 1|       2|
 *   | 1|       3|
 *   +--+--------+
 */
function array_expand(Expression $expression) : Expression
{
    return new Expression\ArrayExpand($expression);
}

function size(Expression $expression) : Expression
{
    return new Expression\Size($expression);
}

function uuid_v4() : Expression
{
    return Expression\Uuid::uuid4();
}

function uuid_v7(?Expression $expression = null) : Expression
{
    return Expression\Uuid::uuid7($expression);
}

function ulid(?Expression $expression = null) : Expression
{
    return new Expression\Ulid($expression);
}

function lower(Expression $expression) : Expression
{
    return new Expression\ToLower($expression);
}

function upper(Expression $expression) : Expression
{
    return new Expression\ToUpper($expression);
}

function call_method(Expression $object, Expression $method, Expression ...$params) : Expression
{
    return new Expression\CallMethod($object, $method, ...$params);
}

function array_sort(Expression $expression, \Closure $function = null) : Expression
{
    return new Expression\ArraySort($expression, $function ?? \Closure::fromCallable('sort'));
}

function not(Expression $expression) : Expression
{
    return new Expression\Not($expression);
}

function to_timezone(Expression $expression, Expression $timeZone) : Expression
{
    return new Expression\ToTimeZone($expression, $timeZone);
}

function to_money(Expression $amount, Expression $currency, ?\Money\MoneyParser $moneyParser = null) : Expression
{
    if (null !== $moneyParser) {
        return new Expression\ToMoney($amount, $currency, $moneyParser);
    }

    return new Expression\ToMoney($amount, $currency);
}

function regex_replace(Expression $pattern, Expression $replacement, Expression $subject) : Expression
{
    return new Expression\PregReplace($pattern, $replacement, $subject);
}

function regex_match_all(Expression $pattern, Expression $subject, Expression $flags = null) : Expression
{
    return new Expression\PregMatchAll($pattern, $subject, $flags);
}

function regex_match(Expression $pattern, Expression $subject) : Expression
{
    return new Expression\PregMatch($pattern, $subject);
}

function sprintf(Expression $format, Expression ...$args) : Expression
{
    return new Expression\Sprintf($format, ...$args);
}

function sanitize(Expression $expression, Expression $placeholder = null, Expression $skipCharacters = null) : Expression
{
    return new Expression\Sanitize($expression, $placeholder ?: new Expression\Literal('*'), $skipCharacters ?: new Expression\Literal(0));
}

/**
 * @param Expression $expression
 * @param null|Expression $precision
 * @param int<0, max> $mode
 *
 * @return Expression
 */
function round(Expression $expression, Expression $precision = null, int $mode = PHP_ROUND_HALF_UP) : Expression
{
    return new Expression\Round($expression, $precision ?? lit(2), $mode);
}
