<?php declare(strict_types=1);

namespace Flow\ETL\Function;

interface FunctionChain
{
    public function arrayGet(string $path) : self;

    public function arrayGetCollection(string ...$keys) : self;

    public function arrayGetCollectionFirst(string ...$keys) : self;

    public function arrayMerge(ScalarFunction $ref) : self;

    public function arrayMergeCollection() : self;

    public function arrayReverse(bool $preserveKeys = false) : self;

    public function arraySort(?string $function = null, ?int $flags = null, bool $recursive = true) : self;

    public function capitalize() : self;

    public function cast(string $type) : self;

    public function concat(ScalarFunction ...$params) : self;

    public function contains(ScalarFunction $needle) : self;

    public function dateFormat(string $format = 'Y-m-d') : self;

    public function dateTimeFormat(string $format = 'Y-m-d H:i:s') : self;

    public function divide(ScalarFunction $ref) : self;

    public function domNodeAttribute(string $attribute) : self;

    public function domNodeValue() : self;

    public function endsWith(ScalarFunction $needle) : self;

    public function equals(ScalarFunction $ref) : self;

    public function exists() : self;

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
    public function expand(string $expandEntryName = 'element', ArrayExpand\ArrayExpand $expand = \Flow\ETL\Function\ArrayExpand\ArrayExpand::VALUES) : self;

    public function greaterThan(ScalarFunction $ref) : self;

    public function greaterThanEqual(ScalarFunction $ref) : self;

    public function hash(string $algorithm = 'xxh128', bool $binary = false, array $options = []) : self;

    public function isEven() : self;

    public function isFalse() : self;

    public function isIn(ScalarFunction $haystack) : self;

    public function isNotNull() : self;

    public function isNotNumeric() : self;

    public function isNull() : self;

    public function isNumeric() : self;

    public function isOdd() : self;

    public function isTrue() : self;

    /**
     * @param class-string<\Flow\ETL\Row\Entry> ...$entryClass
     */
    public function isType(string ...$entryClass) : self;

    public function jsonDecode(int $flags = JSON_THROW_ON_ERROR) : self;

    public function jsonEncode(int $flags = JSON_THROW_ON_ERROR) : self;

    public function lessThan(ScalarFunction $ref) : self;

    public function lessThanEqual(ScalarFunction $ref) : self;

    public function literal(mixed $value) : self;

    public function lower() : self;

    public function method(ScalarFunction $method, ScalarFunction ...$params) : self;

    public function minus(ScalarFunction $ref) : self;

    public function mod(ScalarFunction $ref) : self;

    public function multiply(ScalarFunction $ref) : self;

    public function notEquals(ScalarFunction $ref) : self;

    public function notSame(ScalarFunction $ref) : self;

    public function numberFormat(?ScalarFunction $decimals = null, ?ScalarFunction $decimalSeparator = null, ?ScalarFunction $thousandsSeparator = null) : self;

    public function plus(ScalarFunction $ref) : self;

    public function power(ScalarFunction $ref) : self;

    public function regexMatch(ScalarFunction $pattern) : self;

    public function regexMatchAll(ScalarFunction $pattern, ?ScalarFunction $flags = null) : self;

    public function regexReplace(ScalarFunction $pattern, ScalarFunction $replacement) : self;

    /**
     * @param ScalarFunction $precision
     * @param int<0, max> $mode
     *
     * @return ScalarFunction
     */
    public function round(ScalarFunction $precision, int $mode = PHP_ROUND_HALF_UP) : self;

    public function same(ScalarFunction $ref) : self;

    public function sanitize(?ScalarFunction $placeholder = null, ?ScalarFunction $skipCharacters = null) : self;

    public function size() : self;

    public function sprintf(ScalarFunction ...$params) : self;

    public function startsWith(ScalarFunction $needle) : self;

    public function strPad(int $length, string $pad_string = ' ', int $type = STR_PAD_RIGHT) : self;

    public function strPadBoth(int $length, string $pad_string = ' ') : self;

    public function strPadLeft(int $length, string $pad_string = ' ') : self;

    public function strPadRight(int $length, string $pad_string = ' ') : self;

    /**
     * @param string|string[] $search
     * @param string|string[] $replace
     */
    public function strReplace(string|array $search, string|array $replace) : self;

    /**
     * @param string $format - current format of the date that will be used to create DateTimeImmutable instance
     * @param \DateTimeZone $timeZone
     *
     * @return ScalarFunction
     */
    public function toDate(string $format = \DateTimeInterface::RFC3339, \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : self;

    /**
     * @param string $format - current format of the date that will be used to create DateTimeImmutable instance
     * @param \DateTimeZone $timeZone
     *
     * @return ScalarFunction
     */
    public function toDateTime(string $format = 'Y-m-d H:i:s', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : self;

    public function trim(Trim\Type $type = Trim\Type::BOTH, string $characters = " \t\n\r\0\x0B") : self;

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
    public function unpack(array $skipKeys = [], ?string $entryPrefix = null) : self;

    public function upper() : self;

    public function xpath(string $string) : self;
}
