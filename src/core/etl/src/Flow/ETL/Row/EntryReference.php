<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use function Flow\ETL\DSL\lit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;
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

/**
 * @implements Reference<array{entry: string, alias: ?string}>
 */
final class EntryReference implements Expression, Reference
{
    private ?string $alias = null;

    private SortOrder $sort = SortOrder::ASC;

    public function __construct(private readonly string $entry)
    {
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

    public function cast(string $type) : Expression
    {
        return new Expressions(new Cast($this, $type));
    }

    public function contains(Expression $needle) : Expression
    {
        return new Expressions(new Contains($this, $needle));
    }

    public function desc() : self
    {
        $this->sort = SortOrder::DESC;

        return $this;
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

    public function eval(Row $row) : mixed
    {
        try {
            return $row->valueOf($this->entry);
        } catch (InvalidArgumentException $e) {
            return null;
        }
    }

    public function greaterThan(Expression $ref) : Expression
    {
        return new Expressions(new GreaterThan($this, $ref));
    }

    public function greaterThanEqual(Expression $ref) : Expression
    {
        return new Expressions(new GreaterThanEqual($this, $ref));
    }

    public function hasAlias() : bool
    {
        return $this->alias !== null;
    }

    public function hash(string $algorithm = 'sha256', bool $binary = false, array $options = []) : Expression
    {
        return new Expressions(new Expression\Hash($this, $algorithm, $binary, $options));
    }

    public function is(Reference $ref) : bool
    {
        return $this->name() === $ref->name();
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

    public function name() : string
    {
        return $this->alias ?? $this->entry;
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

    public function sort() : SortOrder
    {
        return $this->sort;
    }

    public function startsWith(Expression $needle) : Expression
    {
        return new Expressions(new StartsWith($this, $needle));
    }

    public function to() : string
    {
        return $this->entry;
    }
}
