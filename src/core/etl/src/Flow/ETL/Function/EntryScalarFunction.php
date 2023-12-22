<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\lit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function;
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;

trait EntryScalarFunction
{
    public function arrayGet(string $path) : ScalarFunction|EntryReference
    {
        return new Function\ArrayGet($this, $path);
    }

    public function arrayGetCollection(string ...$keys) : ScalarFunction|EntryReference
    {
        return new Function\ArrayGetCollection($this, $keys);
    }

    public function arrayGetCollectionFirst(string ...$keys) : ScalarFunction|EntryReference
    {
        return Function\ArrayGetCollection::fromFirst($this, $keys);
    }

    public function arrayMerge(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new Function\ArrayMerge($this, $ref);
    }

    public function arrayMergeCollection() : ScalarFunction|EntryReference
    {
        return new Function\ArrayMergeCollection($this);
    }

    public function arrayReverse(bool $preserveKeys = false) : ScalarFunction|EntryReference
    {
        return new Function\ArrayReverse($this, $preserveKeys);
    }

    public function arraySort(?string $function = null, ?int $flags = null, bool $recursive = true) : ScalarFunction|EntryReference
    {
        return new Function\ArraySort($this, $function ? Sort::fromString($function) : Sort::sort, $flags, $recursive);
    }

    public function capitalize() : ScalarFunction|EntryReference
    {
        return new Function\Capitalize($this);
    }

    public function cast(string $type) : ScalarFunction|EntryReference
    {
        return new Cast($this, $type);
    }

    public function concat(ScalarFunction ...$params) : ScalarFunction|EntryReference
    {
        return new Function\Concat($this, ...$params);
    }

    public function contains(ScalarFunction $needle) : ScalarFunction|EntryReference
    {
        return new Contains($this, $needle);
    }

    public function dateFormat(string $format = 'Y-m-d') : ScalarFunction|EntryReference
    {
        return new Function\DateTimeFormat($this, $format);
    }

    public function dateTimeFormat(string $format = 'Y-m-d H:i:s') : ScalarFunction|EntryReference
    {
        return new Function\DateTimeFormat($this, $format);
    }

    public function divide(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new Divide($this, $ref);
    }

    public function domNodeAttribute(string $attribute) : ScalarFunction|EntryReference
    {
        return new Function\DOMNodeAttribute($this, $attribute);
    }

    public function domNodeValue() : ScalarFunction|EntryReference
    {
        return new Function\DOMNodeValue($this);
    }

    public function endsWith(ScalarFunction $needle) : ScalarFunction|EntryReference
    {
        return new EndsWith($this, $needle);
    }

    public function equals(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new Equals($this, $ref);
    }

    public function exists() : ScalarFunction|EntryReference
    {
        return new Function\Exists($this);
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
    public function expand(string $expandEntryName = 'element', ArrayExpand $expand = ArrayExpand::VALUES) : ScalarFunction|EntryReference
    {
        return new Function\ArrayExpand($this, $expand);
    }

    public function greaterThan(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new GreaterThan($this, $ref);
    }

    public function greaterThanEqual(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new GreaterThanEqual($this, $ref);
    }

    public function hash(string $algorithm = 'xxh128', bool $binary = false, array $options = []) : ScalarFunction|EntryReference
    {
        return new Function\Hash($this, $algorithm, $binary, $options);
    }

    public function isEven() : ScalarFunction|EntryReference
    {
        return new Equals(new Mod($this, lit(2)), lit(0));
    }

    public function isFalse() : ScalarFunction|EntryReference
    {
        return new Same($this, lit(false));
    }

    public function isIn(ScalarFunction $haystack) : ScalarFunction|EntryReference
    {
        return new IsIn($haystack, $this);
    }

    public function isNotNull() : ScalarFunction|EntryReference
    {
        return new IsNotNull($this);
    }

    public function isNotNumeric() : ScalarFunction|EntryReference
    {
        return new IsNotNumeric($this);
    }

    public function isNull() : ScalarFunction|EntryReference
    {
        return new IsNull($this);
    }

    public function isNumeric() : ScalarFunction|EntryReference
    {
        return new IsNumeric($this);
    }

    public function isOdd() : ScalarFunction|EntryReference
    {
        return new NotEquals(new Mod($this, lit(2)), lit(0));
    }

    public function isTrue() : ScalarFunction|EntryReference
    {
        return new Same($this, lit(true));
    }

    /**
     * @param class-string<Entry> ...$entryClass
     */
    public function isType(string ...$entryClass) : ScalarFunction|EntryReference
    {
        if ([] === $entryClass) {
            throw new InvalidArgumentException('isType expression requires at least one entryClass');
        }

        return new IsType($this, ...$entryClass);
    }

    public function jsonDecode(int $flags = JSON_THROW_ON_ERROR) : ScalarFunction|EntryReference
    {
        return new Function\JsonDecode($this, $flags);
    }

    public function jsonEncode(int $flags = JSON_THROW_ON_ERROR) : ScalarFunction|EntryReference
    {
        return new Function\JsonEncode($this, $flags);
    }

    public function lessThan(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new LessThan($this, $ref);
    }

    public function lessThanEqual(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new LessThanEqual($this, $ref);
    }

    public function literal(mixed $value) : ScalarFunction|EntryReference
    {
        return new Literal($value);
    }

    public function lower() : ScalarFunction|EntryReference
    {
        return new Function\ToLower($this);
    }

    public function method(ScalarFunction $method, ScalarFunction ...$params) : ScalarFunction|EntryReference
    {
        return new Function\CallMethod($this, $method, ...$params);
    }

    public function minus(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new Minus($this, $ref);
    }

    public function mod(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new Mod($this, $ref);
    }

    public function multiply(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new Multiply($this, $ref);
    }

    public function notEquals(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new NotEquals($this, $ref);
    }

    public function notSame(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new NotSame($this, $ref);
    }

    public function numberFormat(?ScalarFunction $decimals = null, ?ScalarFunction $decimalSeparator = null, ?ScalarFunction $thousandsSeparator = null) : ScalarFunction|EntryReference
    {
        if ($decimals === null) {
            $decimals = lit(0);
        }

        if ($decimalSeparator === null) {
            $decimalSeparator = lit('.');
        }

        if ($thousandsSeparator === null) {
            $thousandsSeparator = lit(',');
        }

        return new Function\NumberFormat($this, $decimals, $decimalSeparator, $thousandsSeparator);
    }

    public function plus(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new Plus($this, $ref);
    }

    public function power(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new Power($this, $ref);
    }

    public function regexMatch(ScalarFunction $pattern) : ScalarFunction|EntryReference
    {
        return new Function\PregMatch($pattern, $this);
    }

    public function regexMatchAll(ScalarFunction $pattern, ?ScalarFunction $flags = null) : ScalarFunction|EntryReference
    {
        return new Function\PregMatchAll($pattern, $this, $flags);
    }

    public function regexReplace(ScalarFunction $pattern, ScalarFunction $replacement) : ScalarFunction|EntryReference
    {
        return new Function\PregReplace($pattern, $replacement, $this);
    }

    /**
     * @param ScalarFunction $precision
     * @param int<0, max> $mode
     *
     * @return ScalarFunction
     */
    public function round(ScalarFunction $precision, int $mode = PHP_ROUND_HALF_UP) : ScalarFunction|EntryReference
    {
        return new Function\Round($this, $precision, $mode);
    }

    public function same(ScalarFunction $ref) : ScalarFunction|EntryReference
    {
        return new Same($this, $ref);
    }

    public function sanitize(?ScalarFunction $placeholder = null, ?ScalarFunction $skipCharacters = null) : ScalarFunction|EntryReference
    {
        return new Function\Sanitize($this, $placeholder ?: new Function\Literal('*'), $skipCharacters ?: new Function\Literal(0));
    }

    public function size() : ScalarFunction|EntryReference
    {
        return new Function\Size($this);
    }

    public function sprintf(ScalarFunction ...$params) : ScalarFunction|EntryReference
    {
        return new Function\Sprintf($this, ...$params);
    }

    public function startsWith(ScalarFunction $needle) : ScalarFunction|EntryReference
    {
        return new StartsWith($this, $needle);
    }

    public function strPad(int $length, string $pad_string = ' ', int $type = STR_PAD_RIGHT) : ScalarFunction|EntryReference
    {
        return new Function\StrPad($this, $length, $pad_string, $type);
    }

    public function strPadBoth(int $length, string $pad_string = ' ') : ScalarFunction|EntryReference
    {
        return new Function\StrPad($this, $length, $pad_string, STR_PAD_BOTH);
    }

    public function strPadLeft(int $length, string $pad_string = ' ') : ScalarFunction|EntryReference
    {
        return new Function\StrPad($this, $length, $pad_string, STR_PAD_LEFT);
    }

    public function strPadRight(int $length, string $pad_string = ' ') : ScalarFunction|EntryReference
    {
        return new Function\StrPad($this, $length, $pad_string, STR_PAD_RIGHT);
    }

    /**
     * @param string|string[] $search
     * @param string|string[] $replace
     */
    public function strReplace(string|array $search, string|array $replace) : ScalarFunction|EntryReference
    {
        return new Function\StrReplace($this, $search, $replace);
    }

    /**
     * @param string $format - current format of the date that will be used to create DateTimeImmutable instance
     * @param \DateTimeZone $timeZone
     *
     * @return ScalarFunction
     */
    public function toDate(string $format = \DateTimeInterface::RFC3339, \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ScalarFunction|EntryReference
    {
        return new Function\ToDate($this, $format, $timeZone);
    }

    /**
     * @param string $format - current format of the date that will be used to create DateTimeImmutable instance
     * @param \DateTimeZone $timeZone
     *
     * @return ScalarFunction
     */
    public function toDateTime(string $format = 'Y-m-d H:i:s', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ScalarFunction|EntryReference
    {
        return new Function\ToDateTime($this, $format, $timeZone);
    }

    public function trim(Trim\Type $type = Trim\Type::BOTH, string $characters = " \t\n\r\0\x0B") : ScalarFunction|EntryReference
    {
        return new Function\Trim($this, $type, $characters);
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
    public function unpack(array $skipKeys = [], ?string $entryPrefix = null) : ScalarFunction|EntryReference
    {
        return new Function\ArrayUnpack($this, $skipKeys, $entryPrefix);
    }

    public function upper() : ScalarFunction|EntryReference
    {
        return new Function\ToUpper($this);
    }

    public function xpath(string $string) : ScalarFunction|EntryReference
    {
        return new Function\XPath($this, $string);
    }
}
