<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\lit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function;
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Hash\{Algorithm, NativePHPHash};
use Flow\ETL\PHP\Type\Type;

abstract class ScalarFunctionChain implements ScalarFunction
{
    public function arrayGet(ScalarFunction|string $path) : self
    {
        return new ArrayGet($this, $path);
    }

    public function arrayGetCollection(ScalarFunction|array $keys) : self
    {
        return new ArrayGetCollection($this, $keys);
    }

    public function arrayGetCollectionFirst(string ...$keys) : self
    {
        return ArrayGetCollection::fromFirst($this, $keys);
    }

    public function arrayMerge(ScalarFunction|array $ref) : self
    {
        return new ArrayMerge($this, $ref);
    }

    public function arrayMergeCollection() : self
    {
        return new ArrayMergeCollection($this);
    }

    public function arrayPathExists(ScalarFunction|string $path) : self
    {
        return new ArrayPathExists($this, $path);
    }

    public function arrayReverse(ScalarFunction|bool $preserveKeys = false) : self
    {
        return new ArrayReverse($this, $preserveKeys);
    }

    public function arraySort(ScalarFunction|Sort|null $sortFunction = null, ScalarFunction|int|null $flags = null, ScalarFunction|bool $recursive = true) : self
    {
        return new ArraySort($this, $sortFunction ?? Sort::sort, $flags, $recursive);
    }

    public function ascii() : self
    {
        return new Ascii($this);
    }

    /**
     * @param mixed|ScalarFunction $lowerBoundRef
     * @param mixed|ScalarFunction $upperBoundRef
     * @param Boundary|ScalarFunction $boundary
     */
    public function between(mixed $lowerBoundRef, mixed $upperBoundRef, ScalarFunction|Boundary $boundary = Boundary::LEFT_INCLUSIVE) : self
    {
        return new Between($this, $lowerBoundRef, $upperBoundRef, $boundary);
    }

    public function capitalize() : self
    {
        return new Capitalize($this);
    }

    /**
     * @param ScalarFunction|string|Type<mixed> $type
     */
    public function cast(ScalarFunction|string|Type $type) : self
    {
        return new Cast($this, $type);
    }

    public function coalesce(ScalarFunction ...$params) : self
    {
        return new Coalesce($this, ...$params);
    }

    public function concat(ScalarFunction|string ...$params) : self
    {
        return new Concat($this, ...$params);
    }

    public function contains(ScalarFunction|string $needle) : self
    {
        return new Contains($this, $needle);
    }

    public function dateFormat(string $format = 'Y-m-d') : self
    {
        return new DateTimeFormat($this, $format);
    }

    public function dateTimeFormat(string $format = 'Y-m-d H:i:s') : self
    {
        return new DateTimeFormat($this, $format);
    }

    public function divide(ScalarFunction|int|float|string $ref) : self
    {
        return new Divide($this, $ref);
    }

    /**
     * @deprecated Use domElementAttributeValue instead
     */
    public function domElementAttribute(ScalarFunction|string $attribute) : self
    {
        return new DOMElementAttributeValue($this, $attribute);
    }

    public function domElementAttributesCount() : self
    {
        return new DOMElementAttributesCount($this);
    }

    public function domElementAttributeValue(ScalarFunction|string $attribute) : self
    {
        return new DOMElementAttributeValue($this, $attribute);
    }

    public function domElementValue() : self
    {
        return new DOMElementValue($this);
    }

    public function endsWith(ScalarFunction|string $needle) : self
    {
        return new EndsWith($this, $needle);
    }

    public function equals(mixed $ref) : self
    {
        return new Equals($this, $ref);
    }

    public function exists() : self
    {
        return new Exists($this);
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
    public function expand(ArrayExpand $expand = ArrayExpand::VALUES) : self
    {
        return new Function\ArrayExpand($this, $expand);
    }

    public function greaterThan(mixed $ref) : self
    {
        return new GreaterThan($this, $ref);
    }

    public function greaterThanEqual(mixed $ref) : self
    {
        return new GreaterThanEqual($this, $ref);
    }

    public function hash(Algorithm $algorithm = new NativePHPHash()) : self
    {
        return new Hash($this, $algorithm);
    }

    public function isEven() : self
    {
        return new Equals(new Mod($this, lit(2)), lit(0));
    }

    public function isFalse() : self
    {
        return new Same($this, lit(false));
    }

    public function isIn(ScalarFunction|array $haystack) : self
    {
        return new IsIn($haystack, $this);
    }

    public function isNotNull() : self
    {
        return new IsNotNull($this);
    }

    public function isNotNumeric() : self
    {
        return new IsNotNumeric($this);
    }

    public function isNull() : self
    {
        return new IsNull($this);
    }

    public function isNumeric() : self
    {
        return new IsNumeric($this);
    }

    public function isOdd() : self
    {
        return new NotEquals(new Mod($this, lit(2)), lit(0));
    }

    public function isTrue() : self
    {
        return new Same($this, lit(true));
    }

    /**
     * @param string|Type<mixed> $types
     */
    public function isType(string|Type ...$types) : self
    {
        if ([] === $types) {
            throw new InvalidArgumentException('isType expression requires at least one type');
        }

        return new IsType($this, ...$types);
    }

    public function jsonDecode(ScalarFunction|int $flags = JSON_THROW_ON_ERROR) : self
    {
        return new JsonDecode($this, $flags);
    }

    public function jsonEncode(ScalarFunction|int $flags = JSON_THROW_ON_ERROR) : self
    {
        return new JsonEncode($this, $flags);
    }

    public function lessThan(mixed $ref) : self
    {
        return new LessThan($this, $ref);
    }

    public function lessThanEqual(ScalarFunction $ref) : self
    {
        return new LessThanEqual($this, $ref);
    }

    public function literal(mixed $value) : self
    {
        return new Literal($value);
    }

    public function lower() : self
    {
        return new ToLower($this);
    }

    public function minus(ScalarFunction|int|float $ref) : self
    {
        return new Minus($this, $ref);
    }

    public function mod(ScalarFunction|int|float $value) : self
    {
        return new Mod($this, $value);
    }

    public function multiply(ScalarFunction|int|float $value) : self
    {
        return new Multiply($this, $value);
    }

    public function notEquals(mixed $value) : self
    {
        return new NotEquals($this, $value);
    }

    public function notSame(mixed $value) : self
    {
        return new NotSame($this, $value);
    }

    public function numberFormat(ScalarFunction|int $decimals = 2, ScalarFunction|string $decimalSeparator = '.', ScalarFunction|string $thousandsSeparator = ',') : self
    {
        return new NumberFormat($this, $decimals, $decimalSeparator, $thousandsSeparator);
    }

    /**
     * Execute a scalar function on each element of an array/list/map/structure entry.
     * In order to use this function, you need to provide a reference to the "element" that will be used in the function.
     *
     * Example: $df->withEntry('array', ref('array')->onEach(ref('element')->cast(type_string())))
     */
    public function onEach(ScalarFunction $function, ScalarFunction|bool $preserveKeys = true) : OnEach
    {
        return new OnEach($this, $function, $preserveKeys);
    }

    public function plus(ScalarFunction|int|float $ref) : self
    {
        return new Plus($this, $ref);
    }

    public function power(ScalarFunction|int|float $value) : self
    {
        return new Power($this, $value instanceof ScalarFunction ? $value : lit($value));
    }

    public function regex(ScalarFunction|string $pattern, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : self
    {
        return new Regex($pattern, $this, $flags, $offset);
    }

    public function regexAll(ScalarFunction|string $pattern, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : RegexAll
    {
        return new RegexAll($pattern, $this, $flags, $offset);
    }

    public function regexMatch(ScalarFunction|string $pattern, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : self
    {
        return new RegexMatch($pattern, $this, $flags, $offset);
    }

    public function regexMatchAll(ScalarFunction|string $pattern, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : self
    {
        return new RegexMatchAll($pattern, $this, $flags, $offset);
    }

    public function regexReplace(ScalarFunction|string $pattern, ScalarFunction|string $replacement, ScalarFunction|int|null $limit = null) : self
    {
        return new RegexReplace($pattern, $replacement, $this, $limit);
    }

    public function round(ScalarFunction|int $precision = 2, ScalarFunction|int $mode = PHP_ROUND_HALF_UP) : self
    {
        return new Round($this, $precision, $mode);
    }

    public function same(mixed $value) : self
    {
        return new Same($this, $value);
    }

    public function sanitize(ScalarFunction|string $placeholder = '*', ScalarFunction|int|null $skipCharacters = null) : self
    {
        return new Sanitize($this, $placeholder, $skipCharacters);
    }

    public function size() : self
    {
        return new Size($this);
    }

    public function slug(ScalarFunction|string $separator = '-', ScalarFunction|string|null $locale = null, ScalarFunction|array|null $symbolsMap = null) : self
    {
        return new Slug($this, $separator, $locale, $symbolsMap);
    }

    public function split(ScalarFunction|string $separator, ScalarFunction|int $limit = PHP_INT_MAX) : self
    {
        return new Split($this, $separator, $limit);
    }

    public function sprintf(ScalarFunction|float|int|string|null ...$params) : self
    {
        return new Sprintf($this, ...$params);
    }

    public function startsWith(ScalarFunction|string $needle) : self
    {
        return new StartsWith($this, $needle);
    }

    public function strPad(int $length, string $pad_string = ' ', int $type = STR_PAD_RIGHT) : self
    {
        return new StrPad($this, $length, $pad_string, $type);
    }

    public function strPadBoth(int $length, string $pad_string = ' ') : self
    {
        return new StrPad($this, $length, $pad_string, STR_PAD_BOTH);
    }

    public function strPadLeft(int $length, string $pad_string = ' ') : self
    {
        return new StrPad($this, $length, $pad_string, STR_PAD_LEFT);
    }

    public function strPadRight(int $length, string $pad_string = ' ') : self
    {
        return new StrPad($this, $length, $pad_string, STR_PAD_RIGHT);
    }

    /**
     * @param ScalarFunction|string|string[] $search
     * @param ScalarFunction|string|string[] $replace
     */
    public function strReplace(ScalarFunction|string|array $search, ScalarFunction|string|array $replace) : self
    {
        return new StrReplace($this, $search, $replace);
    }

    /**
     * @param ScalarFunction|string $format - current format of the date that will be used to create DateTimeImmutable instance
     */
    public function toDate(ScalarFunction|string $format = \DateTimeInterface::RFC3339, ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC')) : self
    {
        return new ToDate($this, $format, $timeZone);
    }

    /**
     * @param ScalarFunction|string $format - current format of the date that will be used to create DateTimeImmutable instance
     * @param \DateTimeZone|ScalarFunction $timeZone
     */
    public function toDateTime(ScalarFunction|string $format = 'Y-m-d H:i:s', ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC')) : self
    {
        return new ToDateTime($this, $format, $timeZone);
    }

    public function trim(Trim\Type $type = Trim\Type::BOTH, string $characters = " \t\n\r\0\x0B") : self
    {
        return new Trim($this, $type, $characters);
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
    public function unpack(ScalarFunction|array $skipKeys = [], ScalarFunction|string|null $entryPrefix = null) : self
    {
        return new ArrayUnpack($this, $skipKeys, $entryPrefix);
    }

    public function upper() : self
    {
        return new ToUpper($this);
    }

    public function xpath(string $string) : self
    {
        return new XPath($this, $string);
    }
}
