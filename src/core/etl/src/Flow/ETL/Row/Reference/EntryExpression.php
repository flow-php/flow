<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference;

use function Flow\ETL\DSL\lit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
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
use Flow\ETL\Row\Reference\Expression\IsNull;
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
    public function arraySort(\Closure $function = null) : Expression
    {
        return new Expressions(new Expression\ArraySort($this, $function ?? \Closure::fromCallable('asort')));
    }

    public function cast(string $type) : Expression
    {
        return new Expressions(new Cast($this, $type));
    }

    public function contains(Expression $needle) : Expression
    {
        return new Expressions(new Contains($this, $needle));
    }

    public function count() : Expression
    {
        return new Expressions(new Expression\Count($this));
    }

    public function divide(Expression $ref) : Expression
    {
        return new Expressions(new Divide($this, $ref));
    }

    public function endsWith(Expression $needle) : Expression
    {
        return new Expressions(new EndsWith($this, $needle));
    }

    public function equals(Expression $ref) : Expression
    {
        return new Expressions(new Equals($this, $ref));
    }

    public function exists() : Expression
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
    public function expand(string $expandEntryName = 'element') : Expression
    {
        return new Expressions(new Expression\ArrayExpand($this));
    }

    public function greaterThan(Expression $ref) : Expression
    {
        return new Expressions(new GreaterThan($this, $ref));
    }

    public function greaterThanEqual(Expression $ref) : Expression
    {
        return new Expressions(new GreaterThanEqual($this, $ref));
    }

    public function hash(string $algorithm = 'xxh128', bool $binary = false, array $options = []) : Expression
    {
        return new Expressions(new Expression\Hash($this, $algorithm, $binary, $options));
    }

    public function isEven() : Expression
    {
        return new Expressions(new Mod($this, lit(2)), new Equals($this, lit(0)));
    }

    public function isIn(Expression $haystack) : Expression
    {
        return new Expressions(new IsIn($haystack, $this));
    }

    public function isNotNull() : Expression
    {
        return new Expressions(new IsNotNull($this));
    }

    public function isNull() : Expression
    {
        return new Expressions(new IsNull($this));
    }

    public function isOdd() : Expression
    {
        return new Expressions(new Mod($this, lit(2)), new NotEquals($this, lit(0)));
    }

    /**
     * @param class-string<Entry> ...$entryClass
     */
    public function isType(string ...$entryClass) : Expression
    {
        if (!\count($entryClass)) {
            throw new InvalidArgumentException('isType expression requires at least one entryClass');
        }

        return new Expressions(new IsType($this, ...$entryClass));
    }

    public function jsonDecode(int $flags = JSON_THROW_ON_ERROR) : Expression
    {
        return new Expressions(new Expression\JsonDecode($this));
    }

    public function jsonEncode(int $flags = JSON_THROW_ON_ERROR) : Expression
    {
        return new Expressions(new Expression\JsonEncode($this, $flags));
    }

    public function lessThan(Expression $ref) : Expression
    {
        return new Expressions(new LessThan($this, $ref));
    }

    public function lessThanEqual(Expression $ref) : Expression
    {
        return new Expressions(new LessThanEqual($this, $ref));
    }

    public function literal(mixed $value) : Expression
    {
        return new Expressions(new Literal($value));
    }

    public function lower() : Expression
    {
        return new Expressions(new Expression\ToLower($this));
    }

    public function minus(Expression $ref) : Expression
    {
        return new Expressions(new Minus($this, $ref));
    }

    public function mod(Expression $ref) : Expression
    {
        return new Expressions(new Mod($this, $ref));
    }

    public function multiply(Expression $ref) : Expression
    {
        return new Expressions(new Multiply($this, $ref));
    }

    public function notEquals(Expression $ref) : Expression
    {
        return new Expressions(new NotEquals($this, $ref));
    }

    public function notSame(Expression $ref) : Expression
    {
        return new Expressions(new NotSame($this, $ref));
    }

    public function plus(Expression $ref) : Expression
    {
        return new Expressions(new Plus($this, $ref));
    }

    public function power(Expression $ref) : Expression
    {
        return new Expressions(new Power($this, $ref));
    }

    public function same(Expression $ref) : Expression
    {
        return new Expressions(new Same($this, $ref));
    }

    public function size() : Expression
    {
        return new Expressions(new Expression\Size($this));
    }

    public function startsWith(Expression $needle) : Expression
    {
        return new Expressions(new StartsWith($this, $needle));
    }

    public function strReplace(string $search, string $replace) : Expression
    {
        return new Expressions(new Expression\StrReplace($this, $search, $replace));
    }

    /**
     * @param int<0, 2> $type
     */
    public function trim(int $type = Trim::BOTH, string $characters = " \t\n\r\0\x0B") : Expression
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
    public function unpack(array $skipKeys = [], ?string $entryPrefix = null) : Expression
    {
        return new Expressions(new Expression\ArrayUnpack($this, $skipKeys, $entryPrefix));
    }

    public function upper() : Expression
    {
        return new Expressions(new Expression\ToUpper($this));
    }
}
