<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\lit;
use Flow\Calculator\Rounding;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function;
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\Between\Boundary;
use Flow\ETL\Function\StyleConverter\StringStyles as OldStringStyles;
use Flow\ETL\Hash\{Algorithm, NativePHPHash};
use Flow\ETL\String\StringStyles;
use Flow\Types\Type;

abstract class ScalarFunctionChain implements ScalarFunction
{
    public function and(ScalarFunction $function) : All
    {
        return new All($this, $function);
    }

    public function andNot(ScalarFunction $function) : All
    {
        return new All($this, new Not($function));
    }

    public function append(ScalarFunction|string $suffix) : Append
    {
        return new Append($this, $suffix);
    }

    /**
     * Filters an array by removing all elements that matches passed value.
     * Applicable to all data structures that can be converted to an array:
     *    - json
     *    - list
     *    - map
     *    - structure.
     */
    public function arrayFilter(mixed $value) : ArrayFilter
    {
        return new ArrayFilter($this, $value);
    }

    public function arrayGet(ScalarFunction|string $path) : ArrayGet
    {
        return new ArrayGet($this, $path);
    }

    /**
     * @param array<array-key, mixed> $keys
     */
    public function arrayGetCollection(ScalarFunction|array $keys) : ArrayGetCollection
    {
        return new ArrayGetCollection($this, $keys);
    }

    public function arrayGetCollectionFirst(string ...$keys) : ArrayGetCollection
    {
        return ArrayGetCollection::fromFirst($this, $keys);
    }

    /**
     * Filters an array by keeping only elements that matches passed value.
     * Applicable to all data structures that can be converted to an array:
     *   - json
     *   - list
     *   - map
     *   - structure.
     */
    public function arrayKeep(mixed $value) : ArrayKeep
    {
        return new ArrayKeep($this, $value);
    }

    /**
     * Returns all keys from an array, ignoring the values.
     * Applicable to all data structures that can be converted to an array:
     *   - json
     *   - list
     *   - map
     *   - structure.
     */
    public function arrayKeys() : ArrayKeys
    {
        return new ArrayKeys($this);
    }

    /**
     * @param array<array-key, mixed> $ref
     */
    public function arrayMerge(ScalarFunction|array $ref) : ArrayMerge
    {
        return new ArrayMerge($this, $ref);
    }

    public function arrayMergeCollection() : ArrayMergeCollection
    {
        return new ArrayMergeCollection($this);
    }

    public function arrayPathExists(ScalarFunction|string $path) : ArrayPathExists
    {
        return new ArrayPathExists($this, $path);
    }

    public function arrayReverse(ScalarFunction|bool $preserveKeys = false) : ArrayReverse
    {
        return new ArrayReverse($this, $preserveKeys);
    }

    public function arraySort(ScalarFunction|Sort|null $sortFunction = null, ScalarFunction|int|null $flags = null, ScalarFunction|bool $recursive = true) : ArraySort
    {
        return new ArraySort($this, $sortFunction ?? Sort::sort, $flags, $recursive);
    }

    /**
     * Returns all values from an array, ignoring the keys.
     * Applicable to all data structures that can be converted to an array:
     *   - json
     *   - list
     *   - map
     *   - structure.
     */
    public function arrayValues() : ArrayValues
    {
        return new ArrayValues($this);
    }

    public function ascii() : Ascii
    {
        return new Ascii($this);
    }

    /**
     * @param mixed|ScalarFunction $lowerBoundRef
     * @param mixed|ScalarFunction $upperBoundRef
     * @param Boundary|ScalarFunction $boundary
     */
    public function between(mixed $lowerBoundRef, mixed $upperBoundRef, ScalarFunction|Boundary $boundary = Boundary::LEFT_INCLUSIVE) : Between
    {
        return new Between($this, $lowerBoundRef, $upperBoundRef, $boundary);
    }

    public function binaryLength() : BinaryLength
    {
        return new BinaryLength($this);
    }

    /**
     * @param array<array-key, mixed> $arguments
     * @param Type<mixed> $returnType
     */
    public function call(ScalarFunction|callable $callable, array $arguments = [], string|int $refAlias = 0, ?Type $returnType = null) : CallUserFunc
    {
        return new CallUserFunc($callable, array_merge($arguments, [$refAlias => $this]), $returnType);
    }

    public function capitalize() : Capitalize
    {
        return new Capitalize($this);
    }

    /**
     * @param string|Type<mixed> $type
     */
    public function cast(string|Type $type) : Cast
    {
        return new Cast($this, $type);
    }

    public function chunk(ScalarFunction|int $size) : Chunk
    {
        return new Chunk($this, $size);
    }

    public function coalesce(ScalarFunction ...$params) : Coalesce
    {
        return new Coalesce($this, ...$params);
    }

    public function codePointLength() : CodePointLength
    {
        return new CodePointLength($this);
    }

    public function collapseWhitespace() : CollapseWhitespace
    {
        return new CollapseWhitespace($this);
    }

    public function concat(ScalarFunction|string ...$params) : Concat
    {
        return new Concat($this, ...$params);
    }

    public function concatWithSeparator(ScalarFunction|string $separator, ScalarFunction|string ...$params) : ConcatWithSeparator
    {
        return new ConcatWithSeparator($separator, $this, ...$params);
    }

    public function contains(ScalarFunction|string $needle) : Contains
    {
        return new Contains($this, $needle);
    }

    public function dateFormat(string $format = 'Y-m-d') : DateTimeFormat
    {
        return new DateTimeFormat($this, $format);
    }

    public function dateTimeFormat(string $format = 'Y-m-d H:i:s') : DateTimeFormat
    {
        return new DateTimeFormat($this, $format);
    }

    public function divide(ScalarFunction|int|float|string $value, ScalarFunction|int|null $scale = null, ScalarFunction|Rounding|null $rounding = null) : Divide
    {
        return new Divide($this, $value, $scale, $rounding);
    }

    /**
     * @deprecated Use domElementAttributeValue instead
     */
    public function domElementAttribute(ScalarFunction|string $attribute) : DOMElementAttributeValue
    {
        return new DOMElementAttributeValue($this, $attribute);
    }

    public function domElementAttributesCount() : DOMElementAttributesCount
    {
        return new DOMElementAttributesCount($this);
    }

    public function domElementAttributeValue(ScalarFunction|string $attribute) : DOMElementAttributeValue
    {
        return new DOMElementAttributeValue($this, $attribute);
    }

    public function domElementValue() : DOMElementValue
    {
        return new DOMElementValue($this);
    }

    public function endsWith(ScalarFunction|string $needle) : EndsWith
    {
        return new EndsWith($this, $needle);
    }

    public function ensureEnd(ScalarFunction|string $suffix) : EnsureEnd
    {
        return new EnsureEnd($this, $suffix);
    }

    public function ensureStart(ScalarFunction|string $prefix) : EnsureStart
    {
        return new EnsureStart($this, $prefix);
    }

    public function equals(mixed $ref) : Equals
    {
        return new Equals($this, $ref);
    }

    public function exists() : Exists
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
    public function expand(ArrayExpand $expand = ArrayExpand::VALUES) : Function\ArrayExpand
    {
        return new Function\ArrayExpand($this, $expand);
    }

    public function greaterThan(mixed $ref) : GreaterThan
    {
        return new GreaterThan($this, $ref);
    }

    public function greaterThanEqual(mixed $ref) : GreaterThanEqual
    {
        return new GreaterThanEqual($this, $ref);
    }

    public function hash(Algorithm $algorithm = new NativePHPHash()) : Hash
    {
        return new Hash($this, $algorithm);
    }

    /**
     * Returns the index of given $needle in string.
     */
    public function indexOf(ScalarFunction|string $needle, ScalarFunction|bool $ignoreCase = false, ScalarFunction|int $offset = 0) : IndexOf
    {
        return new IndexOf($this, $needle, $ignoreCase, $offset);
    }

    /**
     * Returns the last index of given $needle in string.
     */
    public function indexOfLast(ScalarFunction|string $needle, ScalarFunction|bool $ignoreCase = false, ScalarFunction|int $offset = 0) : IndexOfLast
    {
        return new IndexOfLast($this, $needle, $ignoreCase, $offset);
    }

    public function isEmpty() : IsEmpty
    {
        return new IsEmpty($this);
    }

    public function isEven() : Equals
    {
        return new Equals(new Mod($this, lit(2)), lit(0));
    }

    public function isFalse() : Same
    {
        return new Same($this, lit(false));
    }

    /**
     * @param array<array-key, mixed> $haystack
     */
    public function isIn(ScalarFunction|array $haystack) : IsIn
    {
        return new IsIn($haystack, $this);
    }

    public function isNotNull() : IsNotNull
    {
        return new IsNotNull($this);
    }

    public function isNotNumeric() : IsNotNumeric
    {
        return new IsNotNumeric($this);
    }

    public function isNull() : IsNull
    {
        return new IsNull($this);
    }

    public function isNumeric() : IsNumeric
    {
        return new IsNumeric($this);
    }

    public function isOdd() : NotEquals
    {
        return new NotEquals(new Mod($this, lit(2)), lit(0));
    }

    public function isTrue() : Same
    {
        return new Same($this, lit(true));
    }

    /**
     * @param string|Type<mixed> $types
     */
    public function isType(string|Type ...$types) : IsType
    {
        if ([] === $types) {
            throw new InvalidArgumentException('isType expression requires at least one type');
        }

        return new IsType($this, ...$types);
    }

    /**
     * Check string is utf8 and returns true or false.
     */
    public function isUtf8() : IsUtf8
    {
        return new IsUtf8($this);
    }

    public function jsonDecode(ScalarFunction|int $flags = JSON_THROW_ON_ERROR) : JsonDecode
    {
        return new JsonDecode($this, $flags);
    }

    public function jsonEncode(ScalarFunction|int $flags = JSON_THROW_ON_ERROR) : JsonEncode
    {
        return new JsonEncode($this, $flags);
    }

    public function lessThan(mixed $ref) : LessThan
    {
        return new LessThan($this, $ref);
    }

    public function lessThanEqual(ScalarFunction $ref) : LessThanEqual
    {
        return new LessThanEqual($this, $ref);
    }

    public function literal(mixed $value) : Literal
    {
        return new Literal($value);
    }

    public function lower() : ToLower
    {
        return new ToLower($this);
    }

    public function minus(ScalarFunction|int|float $ref) : Minus
    {
        return new Minus($this, $ref);
    }

    public function mod(ScalarFunction|int $value) : Mod
    {
        return new Mod($this, $value);
    }

    public function modifyDateTime(string|ScalarFunction $modifier) : ModifyDateTime
    {
        return new ModifyDateTime($this, $modifier);
    }

    public function multiply(ScalarFunction|int|float $value) : Multiply
    {
        return new Multiply($this, $value);
    }

    public function notEquals(mixed $value) : NotEquals
    {
        return new NotEquals($this, $value);
    }

    public function notSame(mixed $value) : NotSame
    {
        return new NotSame($this, $value);
    }

    public function numberFormat(ScalarFunction|int $decimals = 2, ScalarFunction|string $decimalSeparator = '.', ScalarFunction|string $thousandsSeparator = ',') : NumberFormat
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

    public function or(ScalarFunction $function) : Any
    {
        return new Any($this, $function);
    }

    public function orNot(ScalarFunction $function) : Any
    {
        return new Any($this, new Not($function));
    }

    public function plus(ScalarFunction|int|float $ref) : Plus
    {
        return new Plus($this, $ref);
    }

    public function power(ScalarFunction|int $value) : Power
    {
        return new Power($this, $value);
    }

    public function prepend(ScalarFunction|string $prefix) : Prepend
    {
        return new Prepend($this, $prefix);
    }

    public function regex(ScalarFunction|string $pattern, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : Regex
    {
        return new Regex($pattern, $this, $flags, $offset);
    }

    public function regexAll(ScalarFunction|string $pattern, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : RegexAll
    {
        return new RegexAll($pattern, $this, $flags, $offset);
    }

    public function regexMatch(ScalarFunction|string $pattern, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : RegexMatch
    {
        return new RegexMatch($pattern, $this, $flags, $offset);
    }

    public function regexMatchAll(ScalarFunction|string $pattern, ScalarFunction|int $flags = 0, ScalarFunction|int $offset = 0) : RegexMatchAll
    {
        return new RegexMatchAll($pattern, $this, $flags, $offset);
    }

    public function regexReplace(ScalarFunction|string $pattern, ScalarFunction|string $replacement, ScalarFunction|int|null $limit = null) : RegexReplace
    {
        return new RegexReplace($pattern, $replacement, $this, $limit);
    }

    public function repeat(ScalarFunction|int $times) : Repeat
    {
        return new Repeat($this, $times);
    }

    public function reverse() : Reverse
    {
        return new Reverse($this);
    }

    public function round(ScalarFunction|int $precision = 2, ScalarFunction|int $mode = PHP_ROUND_HALF_UP) : Round
    {
        return new Round($this, $precision, $mode);
    }

    public function same(mixed $value) : Same
    {
        return new Same($this, $value);
    }

    public function sanitize(ScalarFunction|string $placeholder = '*', ScalarFunction|int|null $skipCharacters = null) : Sanitize
    {
        return new Sanitize($this, $placeholder, $skipCharacters);
    }

    public function size() : Size
    {
        return new Size($this);
    }

    /**
     * @param null|array<array-key, mixed> $symbolsMap
     */
    public function slug(ScalarFunction|string $separator = '-', ScalarFunction|string|null $locale = null, ScalarFunction|array|null $symbolsMap = null) : Slug
    {
        return new Slug($this, $separator, $locale, $symbolsMap);
    }

    public function split(ScalarFunction|string $separator, ScalarFunction|int $limit = PHP_INT_MAX) : Split
    {
        return new Split($this, $separator, $limit);
    }

    public function sprintf(ScalarFunction|float|int|string|null ...$params) : Sprintf
    {
        return new Sprintf($this, ...$params);
    }

    public function startsWith(ScalarFunction|string $needle) : StartsWith
    {
        return new StartsWith($this, $needle);
    }

    /**
     * Returns the contents found after the first occurrence of the given string.
     */
    public function stringAfter(ScalarFunction|string $needle, ScalarFunction|bool $includeNeedle = false) : StringAfter
    {
        return new StringAfter($this, $needle, $includeNeedle);
    }

    /**
     * Returns the contents found after the last occurrence of the given string.
     */
    public function stringAfterLast(ScalarFunction|string $needle, ScalarFunction|bool $includeNeedle = false) : StringAfterLast
    {
        return new StringAfterLast($this, $needle, $includeNeedle);
    }

    /**
     * Returns the contents found before the first occurrence of the given string.
     */
    public function stringBefore(ScalarFunction|string $needle, ScalarFunction|bool $includeNeedle = false) : StringBefore
    {
        return new StringBefore($this, $needle, $includeNeedle);
    }

    /**
     * Returns the contents found before the last occurrence of the given string.
     */
    public function stringBeforeLast(ScalarFunction|string $needle, ScalarFunction|bool $includeNeedle = false) : StringBeforeLast
    {
        return new StringBeforeLast($this, $needle, $includeNeedle);
    }

    /**
     * @param array<string>|ScalarFunction $needles
     */
    public function stringContainsAny(ScalarFunction|array $needles) : StringContainsAny
    {
        return new StringContainsAny($this, $needles);
    }

    public function stringEqualsTo(ScalarFunction|string $string) : StringEqualsTo
    {
        return new StringEqualsTo($this, $string);
    }

    /**
     * Returns a string that you can use in case-insensitive comparisons.
     */
    public function stringFold() : StringFold
    {
        return new StringFold($this);
    }

    public function stringMatch(ScalarFunction|string $pattern) : StringMatch
    {
        return new StringMatch($this, $pattern);
    }

    public function stringMatchAll(ScalarFunction|string $pattern) : StringMatchAll
    {
        return new StringMatchAll($this, $pattern);
    }

    public function stringNormalize(ScalarFunction|int $form = \Normalizer::NFC) : StringNormalize
    {
        return new StringNormalize($this, $form);
    }

    /**
     * Covert string to a style from enum list, passed in parameter.
     * Can be string "upper" or StringStyles::UPPER for Upper (example).
     */
    public function stringStyle(ScalarFunction|string|OldStringStyles|StringStyles $style) : StringStyle
    {
        return new StringStyle($this, $style);
    }

    /**
     * Changes all graphemes/code points to "title case".
     */
    public function stringTitle(ScalarFunction|bool $allWords = false) : StringTitle
    {
        return new StringTitle($this, $allWords);
    }

    public function stringWidth() : StringWidth
    {
        return new StringWidth($this);
    }

    public function strPad(int $length, string $pad_string = ' ', int $type = STR_PAD_RIGHT) : StrPad
    {
        return new StrPad($this, $length, $pad_string, $type);
    }

    public function strPadBoth(int $length, string $pad_string = ' ') : StrPad
    {
        return new StrPad($this, $length, $pad_string, STR_PAD_BOTH);
    }

    public function strPadLeft(int $length, string $pad_string = ' ') : StrPad
    {
        return new StrPad($this, $length, $pad_string, STR_PAD_LEFT);
    }

    public function strPadRight(int $length, string $pad_string = ' ') : StrPad
    {
        return new StrPad($this, $length, $pad_string, STR_PAD_RIGHT);
    }

    /**
     * @param array<string>|ScalarFunction|string $search
     * @param array<string>|ScalarFunction|string $replace
     */
    public function strReplace(ScalarFunction|string|array $search, ScalarFunction|string|array $replace) : StrReplace
    {
        return new StrReplace($this, $search, $replace);
    }

    /**
     * @param ScalarFunction|string $format - current format of the date that will be used to create DateTimeImmutable instance
     */
    public function toDate(ScalarFunction|string $format = \DateTimeInterface::RFC3339, ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ToDate
    {
        return new ToDate($this, $format, $timeZone);
    }

    /**
     * @param ScalarFunction|string $format - current format of the date that will be used to create DateTimeImmutable instance
     * @param \DateTimeZone|ScalarFunction $timeZone
     */
    public function toDateTime(ScalarFunction|string $format = 'Y-m-d H:i:s', ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC')) : ToDateTime
    {
        return new ToDateTime($this, $format, $timeZone);
    }

    public function trim(Trim\Type $type = Trim\Type::BOTH, string $characters = " \t\n\r\0\x0B") : Trim
    {
        return new Trim($this, $type, $characters);
    }

    public function truncate(ScalarFunction|int $length, ScalarFunction|string $ellipsis = '...') : Truncate
    {
        return new Truncate($this, $length, $ellipsis);
    }

    public function unicodeLength() : UnicodeLength
    {
        return new UnicodeLength($this);
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
    /**
     * @param array<array-key, mixed> $skipKeys
     */
    public function unpack(ScalarFunction|array $skipKeys = [], ScalarFunction|string|null $entryPrefix = null) : ArrayUnpack
    {
        return new ArrayUnpack($this, $skipKeys, $entryPrefix);
    }

    public function upper() : ToUpper
    {
        return new ToUpper($this);
    }

    public function wordwrap(ScalarFunction|int $width, ScalarFunction|string $break = "\n", ScalarFunction|bool $cut = false) : Wordwrap
    {
        return new Wordwrap($this, $width, $break, $cut);
    }

    public function xpath(string $string) : XPath
    {
        return new XPath($this, $string);
    }
}
