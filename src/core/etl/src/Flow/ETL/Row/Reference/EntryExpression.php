<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference;

use function Flow\ETL\DSL\lit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference\Expression\ArraySort\Sort;
use Flow\ETL\Row\Reference\Expression\Cast;
use Flow\ETL\Row\Reference\Expression\Contains;
use Flow\ETL\Row\Reference\Expression\Divide;
use Flow\ETL\Row\Reference\Expression\EndsWith;
use Flow\ETL\Row\Reference\Expression\Equals;
use Flow\ETL\Row\Reference\Expression\Expressions;
use Flow\ETL\Row\Reference\Expression\GreaterThan;
use Flow\ETL\Row\Reference\Expression\GreaterThanEqual;
use Flow\ETL\Row\Reference\Expression\IsIn;
use Flow\ETL\Row\Reference\Expression\IsNotNull;
use Flow\ETL\Row\Reference\Expression\IsNotNumeric;
use Flow\ETL\Row\Reference\Expression\IsNull;
use Flow\ETL\Row\Reference\Expression\IsNumeric;
use Flow\ETL\Row\Reference\Expression\IsType;
use Flow\ETL\Row\Reference\Expression\LessThan;
use Flow\ETL\Row\Reference\Expression\LessThanEqual;
use Flow\ETL\Row\Reference\Expression\Literal;
use Flow\ETL\Row\Reference\Expression\Minus;
use Flow\ETL\Row\Reference\Expression\Mod;
use Flow\ETL\Row\Reference\Expression\Multiply;
use Flow\ETL\Row\Reference\Expression\NotEquals;
use Flow\ETL\Row\Reference\Expression\NotSame;
use Flow\ETL\Row\Reference\Expression\Plus;
use Flow\ETL\Row\Reference\Expression\Power;
use Flow\ETL\Row\Reference\Expression\Same;
use Flow\ETL\Row\Reference\Expression\StartsWith;
use Flow\ETL\Row\Reference\Expression\Trim;

trait EntryExpression
{
    public function arrayGet(string $path) : Expression|EntryReference
    {
        return new Expressions(new Expression\ArrayGet($this, $path));
    }

    public function arrayGetCollection(string ...$keys) : Expression|EntryReference
    {
        return new Expressions(new Expression\ArrayGetCollection($this, $keys));
    }

    public function arrayGetCollectionFirst(string ...$keys) : Expression|EntryReference
    {
        return new Expressions(Expression\ArrayGetCollection::fromFirst($this, $keys));
    }

    public function arrayMerge(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new Expression\ArrayMerge($this, $ref));
    }

    public function arrayMergeCollection() : Expression|EntryReference
    {
        return new Expressions(new Expression\ArrayMergeCollection($this));
    }

    public function arrayReverse(bool $preserveKeys = false) : Expression|EntryReference
    {
        return new Expressions(new Expression\ArrayReverse($this, $preserveKeys));
    }

    public function arraySort(string $function = null, int $flags = null, bool $recursive = true) : Expression|EntryReference
    {
        return new Expressions(new Expression\ArraySort($this, $function ? Sort::fromString($function) : Sort::sort, $flags, $recursive));
    }

    public function cast(string $type) : Expression|EntryReference
    {
        return new Expressions(new Cast($this, $type));
    }

    public function contains(Expression $needle) : Expression|EntryReference
    {
        return new Expressions(new Contains($this, $needle));
    }

    public function count() : Expression|EntryReference
    {
        return new Expressions(new Expression\Count($this));
    }

    public function dateFormat(string $format = 'Y-m-d') : Expression|EntryReference
    {
        return new Expressions(new Expression\DateTimeFormat($this, $format));
    }

    public function dateTimeFormat(string $format = 'Y-m-d H:i:s') : Expression|EntryReference
    {
        return new Expressions(new Expression\DateTimeFormat($this, $format));
    }

    public function divide(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new Divide($this, $ref));
    }

    public function domNodeAttribute(string $attribute) : Expression|EntryReference
    {
        return new Expressions(new Expression\DOMNodeAttribute($this, $attribute));
    }

    public function domNodeValue() : Expression|EntryReference
    {
        return new Expressions(new Expression\DOMNodeValue($this));
    }

    public function endsWith(Expression $needle) : Expression|EntryReference
    {
        return new Expressions(new EndsWith($this, $needle));
    }

    public function equals(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new Equals($this, $ref));
    }

    public function exists() : Expression|EntryReference
    {
        return new Expressions(new Expression\Exists($this));
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
    public function expand(string $expandEntryName = 'element') : Expression|EntryReference
    {
        return new Expressions(new Expression\ArrayExpand($this));
    }

    public function greaterThan(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new GreaterThan($this, $ref));
    }

    public function greaterThanEqual(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new GreaterThanEqual($this, $ref));
    }

    public function hash(string $algorithm = 'xxh128', bool $binary = false, array $options = []) : Expression|EntryReference
    {
        return new Expressions(new Expression\Hash($this, $algorithm, $binary, $options));
    }

    public function isEven() : Expression|EntryReference
    {
        return new Equals(new Mod($this, lit(2)), lit(0));
    }

    public function isIn(Expression $haystack) : Expression|EntryReference
    {
        return new Expressions(new IsIn($haystack, $this));
    }

    public function isNotNull() : Expression|EntryReference
    {
        return new Expressions(new IsNotNull($this));
    }

    public function isNotNumeric() : Expression|EntryReference
    {
        return new Expressions(new IsNotNumeric($this));
    }

    public function isNull() : Expression|EntryReference
    {
        return new Expressions(new IsNull($this));
    }

    public function isNumeric() : Expression|EntryReference
    {
        return new Expressions(new IsNumeric($this));
    }

    public function isOdd() : Expression|EntryReference
    {
        return new NotEquals(new Mod($this, lit(2)), lit(0));
    }

    /**
     * @param class-string<Entry> ...$entryClass
     */
    public function isType(string ...$entryClass) : Expression|EntryReference
    {
        if (!\count($entryClass)) {
            throw new InvalidArgumentException('isType expression requires at least one entryClass');
        }

        return new Expressions(new IsType($this, ...$entryClass));
    }

    public function jsonDecode(int $flags = JSON_THROW_ON_ERROR) : Expression|EntryReference
    {
        return new Expressions(new Expression\JsonDecode($this));
    }

    public function jsonEncode(int $flags = JSON_THROW_ON_ERROR) : Expression|EntryReference
    {
        return new Expressions(new Expression\JsonEncode($this, $flags));
    }

    public function lessThan(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new LessThan($this, $ref));
    }

    public function lessThanEqual(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new LessThanEqual($this, $ref));
    }

    public function literal(mixed $value) : Expression|EntryReference
    {
        return new Expressions(new Literal($value));
    }

    public function lower() : Expression|EntryReference
    {
        return new Expressions(new Expression\ToLower($this));
    }

    public function method(Expression $method, Expression ...$params) : Expression|EntryReference
    {
        return new Expressions(new Expression\CallMethod($this, $method, ...$params));
    }

    public function minus(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new Minus($this, $ref));
    }

    public function mod(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new Mod($this, $ref));
    }

    public function multiply(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new Multiply($this, $ref));
    }

    public function notEquals(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new NotEquals($this, $ref));
    }

    public function notSame(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new NotSame($this, $ref));
    }

    public function plus(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new Plus($this, $ref));
    }

    public function power(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new Power($this, $ref));
    }

    public function regexMatch(Expression $pattern) : Expression|EntryReference
    {
        return new Expressions(new Expression\PregMatch($pattern, $this));
    }

    public function regexMatchAll(Expression $pattern, Expression $flags = null) : Expression|EntryReference
    {
        return new Expressions(new Expression\PregMatchAll($pattern, $this, $flags));
    }

    public function regexReplace(Expression $pattern, Expression $replacement) : Expression|EntryReference
    {
        return new Expressions(new Expression\PregReplace($pattern, $replacement, $this));
    }

    /**
     * @param Expression $precision
     * @param int<0, max> $mode
     *
     * @return Expression
     */
    public function round(Expression $precision, int $mode = PHP_ROUND_HALF_UP) : Expression|EntryReference
    {
        return new Expressions(new Expression\Round($this, $precision, $mode));
    }

    public function same(Expression $ref) : Expression|EntryReference
    {
        return new Expressions(new Same($this, $ref));
    }

    public function sanitize(Expression $placeholder = null, Expression $skipCharacters = null) : Expression|EntryReference
    {
        return new Expressions(new Expression\Sanitize($this, $placeholder ?: new Expression\Literal('*'), $skipCharacters ?: new Expression\Literal(0)));
    }

    public function size() : Expression|EntryReference
    {
        return new Expressions(new Expression\Size($this));
    }

    public function sprintf(Expression ...$params) : Expression|EntryReference
    {
        return new Expressions(new Expression\Sprintf($this, ...$params));
    }

    public function startsWith(Expression $needle) : Expression|EntryReference
    {
        return new Expressions(new StartsWith($this, $needle));
    }

    public function strPad(int $length, string $pad_string = ' ', int $type = STR_PAD_RIGHT) : Expression|EntryReference
    {
        return new Expressions((new Expression\StrPad($this, $length, $pad_string, $type)));
    }

    /**
     * @param string|string[] $search
     * @param string|string[] $replace
     */
    public function strReplace(string|array $search, string|array $replace) : Expression|EntryReference
    {
        return new Expressions(new Expression\StrReplace($this, $search, $replace));
    }

    /**
     * @param string $format - current format of the date that will be used to create DateTimeImmutable instance
     * @param \DateTimeZone $timeZone
     *
     * @return Expression
     */
    public function toDate(string $format = 'Y-m-d', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : Expression|EntryReference
    {
        return new Expressions(new Expression\ToDate($this, $format, $timeZone));
    }

    /**
     * @param string $format - current format of the date that will be used to create DateTimeImmutable instance
     * @param \DateTimeZone $timeZone
     *
     * @return Expression
     */
    public function toDateTime(string $format = 'Y-m-d H:i:s', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : Expression|EntryReference
    {
        return new Expressions(new Expression\ToDateTime($this, $format, $timeZone));
    }

    public function trim(Trim\Type $type = Trim\Type::BOTH, string $characters = " \t\n\r\0\x0B") : Expression|EntryReference
    {
        return new Expressions(new Expression\Trim($this, $type, $characters));
    }

    /**
     * Unpacks each element of an array into a new entry, using the array key as the entry name.
     *
     * Before:
     *   +--+-------------------+
     *   |id|              array|
     *   +--+-------------------+
     *   | 1|{"a":1,"b":2,"c":3}|
     *   | 2|{"d":4,"e":5,"f":6}|
     *   +--+-------------------+
     *
     * After:
     *   +--+-----+-----+-----+-----+-----+
     *   |id|arr.b|arr.c|arr.d|arr.e|arr.f|
     *   +--+-----+-----+-----+-----+-----+
     *   | 1|    2|    3|     |     |     |
     *   | 2|     |     |    4|    5|    6|
     *   +--+-----+-----+-----+-----+-----+
     */
    public function unpack(array $skipKeys = [], ?string $entryPrefix = null) : Expression|EntryReference
    {
        return new Expressions(new Expression\ArrayUnpack($this, $skipKeys, $entryPrefix));
    }

    public function upper() : Expression|EntryReference
    {
        return new Expressions(new Expression\ToUpper($this));
    }

    public function xpath(string $string) : Expression|EntryReference
    {
        return new Expressions(new Expression\XPath($this, $string));
    }
}
