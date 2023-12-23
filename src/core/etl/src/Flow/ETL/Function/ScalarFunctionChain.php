<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\lit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function;
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Row\Entry;

abstract class ScalarFunctionChain implements ScalarFunction
{
    private null|ScalarFunctionChain $parentFunction = null;

    public function arrayGet(string $path) : self
    {
        $function = new Function\ArrayGet($this, $path);
        $function->setParentFunction($this);

        return $function;
    }

    public function arrayGetCollection(string ...$keys) : self
    {
        $function = new Function\ArrayGetCollection($this, $keys);
        $function->setParentFunction($this);

        return $function;
    }

    public function arrayGetCollectionFirst(string ...$keys) : self
    {
        return Function\ArrayGetCollection::fromFirst($this, $keys);
    }

    public function arrayMerge(ScalarFunction $ref) : self
    {
        $function = new Function\ArrayMerge($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function arrayMergeCollection() : self
    {
        $function = new Function\ArrayMergeCollection($this);
        $function->setParentFunction($this);

        return $function;
    }

    public function arrayReverse(bool $preserveKeys = false) : self
    {
        $function = new Function\ArrayReverse($this, $preserveKeys);
        $function->setParentFunction($this);

        return $function;
    }

    public function arraySort(?string $algorithm = null, ?int $flags = null, bool $recursive = true) : self
    {
        $function = new Function\ArraySort($this, $algorithm ? Sort::fromString($algorithm) : Sort::sort, $flags, $recursive);
        $function->setParentFunction($this);

        return $function;
    }

    public function capitalize() : self
    {
        $function = new Function\Capitalize($this);
        $function->setParentFunction($this);

        return $function;
    }

    public function cast(string $type) : self
    {
        $function = new Cast($this, $type);
        $function->setParentFunction($this);

        return $function;
    }

    public function concat(ScalarFunction ...$params) : self
    {
        $function = new Function\Concat($this, ...$params);
        $function->setParentFunction($this);

        return $function;
    }

    public function contains(ScalarFunction $needle) : self
    {
        $function = new Contains($this, $needle);
        $function->setParentFunction($this);

        return $function;
    }

    public function dateFormat(string $format = 'Y-m-d') : self
    {
        $function = new Function\DateTimeFormat($this, $format);
        $function->setParentFunction($this);

        return $function;
    }

    public function dateTimeFormat(string $format = 'Y-m-d H:i:s') : self
    {
        $function = new Function\DateTimeFormat($this, $format);
        $function->setParentFunction($this);

        return $function;
    }

    public function divide(ScalarFunction $ref) : self
    {
        $function = new Divide($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function domNodeAttribute(string $attribute) : self
    {
        $function = new Function\DOMNodeAttribute($this, $attribute);
        $function->setParentFunction($this);

        return $function;
    }

    public function domNodeValue() : self
    {
        $function = new Function\DOMNodeValue($this);
        $function->setParentFunction($this);

        return $function;
    }

    public function endsWith(ScalarFunction $needle) : self
    {
        $function = new EndsWith($this, $needle);
        $function->setParentFunction($this);

        return $function;
    }

    public function equals(ScalarFunction $ref) : self
    {
        $function = new Equals($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function exists() : self
    {
        $function = new Function\Exists($this);
        $function->setParentFunction($this);

        return $function;
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
    public function expand(string $expandEntryName = 'element', ArrayExpand $expand = ArrayExpand::VALUES) : self
    {
        $function = new Function\ArrayExpand($this, $expand);
        $function->setParentFunction($this);

        return $function;
    }

    public function getRootFunction() : ?ScalarFunction
    {
        $parentFunction = $this->parentFunction;

        if ($parentFunction === null) {
            return $this;
        }

        while ($parentFunction->parentFunction !== null) {
            $parentFunction = $parentFunction->parentFunction;
        }

        return $parentFunction;
    }

    public function greaterThan(ScalarFunction $ref) : self
    {
        $function = new GreaterThan($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function greaterThanEqual(ScalarFunction $ref) : self
    {
        $function = new GreaterThanEqual($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function hash(string $algorithm = 'xxh128', bool $binary = false, array $options = []) : self
    {
        $function = new Function\Hash($this, $algorithm, $binary, $options);
        $function->setParentFunction($this);

        return $function;
    }

    public function isEven() : self
    {
        $function = new Equals(new Mod($this, lit(2)), lit(0));
        $function->setParentFunction($this);

        return $function;
    }

    public function isFalse() : self
    {
        $function = new Same($this, lit(false));
        $function->setParentFunction($this);

        return $function;
    }

    public function isIn(ScalarFunction $haystack) : self
    {
        $function = new IsIn($haystack, $this);
        $function->setParentFunction($this);

        return $function;
    }

    public function isNotNull() : self
    {
        $function = new IsNotNull($this);
        $function->setParentFunction($this);

        return $function;
    }

    public function isNotNumeric() : self
    {
        $function = new IsNotNumeric($this);
        $function->setParentFunction($this);

        return $function;
    }

    public function isNull() : self
    {
        $function = new IsNull($this);
        $function->setParentFunction($this);

        return $function;
    }

    public function isNumeric() : self
    {
        $function = new IsNumeric($this);
        $function->setParentFunction($this);

        return $function;
    }

    public function isOdd() : self
    {
        $function = new NotEquals(new Mod($this, lit(2)), lit(0));
        $function->setParentFunction($this);

        return $function;
    }

    public function isTrue() : self
    {
        $function = new Same($this, lit(true));
        $function->setParentFunction($this);

        return $function;
    }

    /**
     * @param class-string<Entry> ...$entryClass
     */
    public function isType(string ...$entryClass) : self
    {
        if ([] === $entryClass) {
            throw new InvalidArgumentException('isType expression requires at least one entryClass');
        }

        $function = new IsType($this, ...$entryClass);
        $function->setParentFunction($this);

        return $function;
    }

    public function jsonDecode(int $flags = JSON_THROW_ON_ERROR) : self
    {
        $function = new Function\JsonDecode($this, $flags);
        $function->setParentFunction($this);

        return $function;
    }

    public function jsonEncode(int $flags = JSON_THROW_ON_ERROR) : self
    {
        $function = new Function\JsonEncode($this, $flags);
        $function->setParentFunction($this);

        return $function;
    }

    public function lessThan(ScalarFunction $ref) : self
    {
        $function = new LessThan($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function lessThanEqual(ScalarFunction $ref) : self
    {
        $function = new LessThanEqual($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function literal(mixed $value) : self
    {
        $function = new Literal($value);
        $function->setParentFunction($this);

        return $function;
    }

    public function lower() : self
    {
        $function = new Function\ToLower($this);
        $function->setParentFunction($this);

        return $function;
    }

    public function method(ScalarFunction $method, ScalarFunction ...$params) : self
    {
        $function = new Function\CallMethod($this, $method, ...$params);
        $function->setParentFunction($this);

        return $function;
    }

    public function minus(ScalarFunction $ref) : self
    {
        $function = new Minus($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function mod(ScalarFunction $ref) : self
    {
        $function = new Mod($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function multiply(ScalarFunction $ref) : self
    {
        $function = new Multiply($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function notEquals(ScalarFunction $ref) : self
    {
        $function = new NotEquals($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function notSame(ScalarFunction $ref) : self
    {
        $function = new NotSame($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function numberFormat(?ScalarFunction $decimals = null, ?ScalarFunction $decimalSeparator = null, ?ScalarFunction $thousandsSeparator = null) : self
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

        $function = new Function\NumberFormat($this, $decimals, $decimalSeparator, $thousandsSeparator);
        $function->setParentFunction($this);

        return $function;
    }

    public function plus(ScalarFunction $ref) : self
    {
        $function = new Plus($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function power(ScalarFunction $ref) : self
    {
        $function = new Power($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function regexMatch(ScalarFunction $pattern) : self
    {
        $function = new Function\PregMatch($pattern, $this);
        $function->setParentFunction($this);

        return $function;
    }

    public function regexMatchAll(ScalarFunction $pattern, ?ScalarFunction $flags = null) : self
    {
        $function = new Function\PregMatchAll($pattern, $this, $flags);
        $function->setParentFunction($this);

        return $function;
    }

    public function regexReplace(ScalarFunction $pattern, ScalarFunction $replacement) : self
    {
        $function = new Function\PregReplace($pattern, $replacement, $this);
        $function->setParentFunction($this);

        return $function;
    }

    /**
     * @param ScalarFunction $precision
     * @param int<0, max> $mode
     */
    public function round(ScalarFunction $precision, int $mode = PHP_ROUND_HALF_UP) : self
    {
        $function = new Function\Round($this, $precision, $mode);
        $function->setParentFunction($this);

        return $function;
    }

    public function same(ScalarFunction $ref) : self
    {
        $function = new Same($this, $ref);
        $function->setParentFunction($this);

        return $function;
    }

    public function sanitize(?ScalarFunction $placeholder = null, ?ScalarFunction $skipCharacters = null) : self
    {
        $function = new Function\Sanitize($this, $placeholder ?: new Function\Literal('*'), $skipCharacters ?: new Function\Literal(0));
        $function->setParentFunction($this);

        return $function;
    }

    public function setParentFunction(self $parentFunction) : void
    {
        $this->parentFunction = $parentFunction;
    }

    public function size() : self
    {
        $function = new Function\Size($this);
        $function->setParentFunction($this);

        return $function;
    }

    public function sprintf(ScalarFunction ...$params) : self
    {
        $function = new Function\Sprintf($this, ...$params);
        $function->setParentFunction($this);

        return $function;
    }

    public function startsWith(ScalarFunction $needle) : self
    {
        $function = new StartsWith($this, $needle);
        $function->setParentFunction($this);

        return $function;
    }

    public function strPad(int $length, string $pad_string = ' ', int $type = STR_PAD_RIGHT) : self
    {
        $function = new Function\StrPad($this, $length, $pad_string, $type);
        $function->setParentFunction($this);

        return $function;
    }

    public function strPadBoth(int $length, string $pad_string = ' ') : self
    {
        $function = new Function\StrPad($this, $length, $pad_string, STR_PAD_BOTH);
        $function->setParentFunction($this);

        return $function;
    }

    public function strPadLeft(int $length, string $pad_string = ' ') : self
    {
        $function = new Function\StrPad($this, $length, $pad_string, STR_PAD_LEFT);
        $function->setParentFunction($this);

        return $function;
    }

    public function strPadRight(int $length, string $pad_string = ' ') : self
    {
        $function = new Function\StrPad($this, $length, $pad_string, STR_PAD_RIGHT);
        $function->setParentFunction($this);

        return $function;
    }

    /**
     * @param string|string[] $search
     * @param string|string[] $replace
     */
    public function strReplace(string|array $search, string|array $replace) : self
    {
        $function = new Function\StrReplace($this, $search, $replace);
        $function->setParentFunction($this);

        return $function;
    }

    /**
     * @param string $format - current format of the date that will be used to create DateTimeImmutable instance
     * @param \DateTimeZone $timeZone
     */
    public function toDate(string $format = \DateTimeInterface::RFC3339, \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : self
    {
        $function = new Function\ToDate($this, $format, $timeZone);
        $function->setParentFunction($this);

        return $function;
    }

    /**
     * @param string $format - current format of the date that will be used to create DateTimeImmutable instance
     * @param \DateTimeZone $timeZone
     */
    public function toDateTime(string $format = 'Y-m-d H:i:s', \DateTimeZone $timeZone = new \DateTimeZone('UTC')) : self
    {
        $function = new Function\ToDateTime($this, $format, $timeZone);
        $function->setParentFunction($this);

        return $function;
    }

    public function trim(Trim\Type $type = Trim\Type::BOTH, string $characters = " \t\n\r\0\x0B") : self
    {
        $function = new Function\Trim($this, $type, $characters);
        $function->setParentFunction($this);

        return $function;
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
    public function unpack(array $skipKeys = [], ?string $entryPrefix = null) : self
    {
        $function = new Function\ArrayUnpack($this, $skipKeys, $entryPrefix);
        $function->setParentFunction($this);

        return $function;
    }

    public function upper() : self
    {
        $function = new Function\ToUpper($this);
        $function->setParentFunction($this);

        return $function;
    }

    public function xpath(string $string) : self
    {
        $function = new Function\XPath($this, $string);
        $function->setParentFunction($this);

        return $function;
    }
}
