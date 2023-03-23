<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
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
use Flow\ETL\Row\Reference\Expression\Value;

/**
 * @implements Reference<array{entry: string, alias: ?string, expressions: Expressions}>
 */
final class EntryReference implements Reference
{
    private ?string $alias = null;

    private Expressions $expressions;

    private SortOrder $sort = SortOrder::ASC;

    public function __construct(private readonly string $entry)
    {
        $this->expressions = new Expressions($entry, [new Value($entry)]);
    }

    public static function init(string|self $ref) : self
    {
        if (\is_string($ref)) {
            return new self($ref);
        }

        return $ref;
    }

    public function __serialize() : array
    {
        return [
            'entry' => $this->entry,
            'alias' => $this->alias,
            'expressions' => $this->expressions,
        ];
    }

    public function __toString() : string
    {
        return $this->name();
    }

    public function __unserialize(array $data) : void
    {
        $this->entry = $data['entry'];
        $this->alias = $data['alias'];
        $this->expressions = $data['expressions'];
    }

    public function as(string $alias) : self
    {
        $this->alias = $alias;

        return $this;
    }

    public function asc() : self
    {
        $this->sort = SortOrder::ASC;

        return $this;
    }

    public function contains(self|string|Literal $needle) : self
    {
        $this->expressions->add(new Contains($this, $needle instanceof Literal ? $needle : self::init($needle)));

        return $this;
    }

    public function desc() : self
    {
        $this->sort = SortOrder::DESC;

        return $this;
    }

    public function divide(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new Divide($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function endsWith(self|string|Literal $needle) : self
    {
        $this->expressions->add(new EndsWith($this, $needle instanceof Literal ? $needle : self::init($needle)));

        return $this;
    }

    public function equals(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new Equals($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function eval(Row $row) : mixed
    {
        return $this->expressions->eval($row);
    }

    public function expressions() : Expressions
    {
        return $this->expressions;
    }

    public function greaterThan(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new GreaterThan($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function greaterThanEqual(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new GreaterThanEqual($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function hasAlias() : bool
    {
        return $this->alias !== null;
    }

    public function is(Reference $ref) : bool
    {
        return $this->name() === $ref->name();
    }

    public function isIn(self|string|Literal $haystack) : self
    {
        $this->expressions->add(new IsIn($haystack instanceof Literal ? $haystack : self::init($haystack), $this));

        return $this;
    }

    public function isNotNull() : self
    {
        $this->expressions = $this->expressions->add(new IsNotNull($this));

        return $this;
    }

    public function isNull() : self
    {
        $this->expressions = $this->expressions->add(new IsNull($this));

        return $this;
    }

    /**
     * @param class-string<Entry> ...$entryClass
     */
    public function isType(string ...$entryClass) : self
    {
        if (!\count($entryClass)) {
            throw new InvalidArgumentException('isType expression requires at least one entryClass');
        }

        $this->expressions = $this->expressions->add(new IsType($this, ...$entryClass));

        return $this;
    }

    public function lessThan(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new LessThan($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function lessThanEqual(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new LessThanEqual($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function literal(mixed $value) : self
    {
        $this->expressions = $this->expressions->add(new Literal($value));

        return $this;
    }

    public function minus(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new Minus($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function mod(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new Mod($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function multiply(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new Multiply($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function name() : string
    {
        return $this->alias ?? $this->entry;
    }

    public function notEquals(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new NotEquals($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function notSame(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new NotSame($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function plus(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new Plus($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function power(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new Power($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function same(self|string|Literal $ref) : self
    {
        $this->expressions = $this->expressions->add(new Same($this, $ref instanceof Literal ? $ref : self::init($ref)));

        return $this;
    }

    public function sort() : SortOrder
    {
        return $this->sort;
    }

    public function startsWith(self|string|Literal $needle) : self
    {
        $this->expressions->add(new StartsWith($this, $needle instanceof Literal ? $needle : self::init($needle)));

        return $this;
    }

    public function to() : string
    {
        return $this->entry;
    }
}
