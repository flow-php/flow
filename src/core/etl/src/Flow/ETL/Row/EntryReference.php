<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Row\Reference\Expression\Contains;
use Flow\ETL\Row\Reference\Expression\Divide;
use Flow\ETL\Row\Reference\Expression\EndsWith;
use Flow\ETL\Row\Reference\Expression\Equals;
use Flow\ETL\Row\Reference\Expression\Expressions;
use Flow\ETL\Row\Reference\Expression\GreaterThan;
use Flow\ETL\Row\Reference\Expression\GreaterThanEqual;
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
 * @implements Reference<array{entry: string, alias: ?string, expression: ?Expressions}>
 */
final class EntryReference implements Reference
{
    private ?string $alias = null;

    private Expressions $expression;

    private SortOrder $sort = SortOrder::ASC;

    public function __construct(private readonly string $entry)
    {
        $this->expression = new Expressions($entry, [new Value($entry)]);
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
            'expression' => $this->expression,
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
        $this->expression = $data['expression'];
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

    public function contains(self|string $needle) : self
    {
        $this->expression->add(new Contains($this, self::init($needle)));

        return $this;
    }

    public function desc() : self
    {
        $this->sort = SortOrder::DESC;

        return $this;
    }

    public function divide(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new Divide($this, self::init($ref)));

        return $this;
    }

    public function endsWith(self|string $needle) : self
    {
        $this->expression->add(new EndsWith($this, self::init($needle)));

        return $this;
    }

    public function equals(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new Equals($this, self::init($ref)));

        return $this;
    }

    public function eval(Row $row) : mixed
    {
        return $this->expression->eval($row);
    }

    public function expressions() : Expressions
    {
        return $this->expression;
    }

    public function greaterThan(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new GreaterThan($this, self::init($ref)));

        return $this;
    }

    public function greaterThanEqual(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new GreaterThanEqual($this, self::init($ref)));

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

    public function isNotNull() : self
    {
        $this->expression = $this->expression->add(new IsNotNull($this));

        return $this;
    }

    public function isNull() : self
    {
        $this->expression = $this->expression->add(new IsNull($this));

        return $this;
    }

    public function isType(string ...$entryClass) : self
    {
        if (!\count($entryClass)) {
            throw new InvalidArgumentException('isType expression requires at least one entryClass');
        }

        $this->expression = $this->expression->add(new IsType($this, ...$entryClass));

        return $this;
    }

    public function lessThan(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new LessThan($this, self::init($ref)));

        return $this;
    }

    public function lessThanEqual(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new LessThanEqual($this, self::init($ref)));

        return $this;
    }

    public function literal(mixed $value) : self
    {
        $this->expression = $this->expression->add(new Literal($value));

        return $this;
    }

    public function minus(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new Minus($this, self::init($ref)));

        return $this;
    }

    public function mod(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new Mod($this, self::init($ref)));

        return $this;
    }

    public function multiply(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new Multiply($this, self::init($ref)));

        return $this;
    }

    public function name() : string
    {
        return $this->alias ?? $this->entry;
    }

    public function notEquals(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new NotEquals($this, self::init($ref)));

        return $this;
    }

    public function notSame(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new NotSame($this, self::init($ref)));

        return $this;
    }

    public function plus(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new Plus($this, self::init($ref)));

        return $this;
    }

    public function power(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new Power($this, self::init($ref)));

        return $this;
    }

    public function same(self|string $ref) : self
    {
        $this->expression = $this->expression->add(new Same($this, self::init($ref)));

        return $this;
    }

    public function sort() : SortOrder
    {
        return $this->sort;
    }

    public function startsWith(self|string $needle) : self
    {
        $this->expression->add(new StartsWith($this, self::init($needle)));

        return $this;
    }

    public function to() : string
    {
        return $this->entry;
    }
}
