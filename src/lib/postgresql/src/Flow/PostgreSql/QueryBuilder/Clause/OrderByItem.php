<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\SortBy;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{Expression, ExpressionFactory};

/**
 * Represents an ORDER BY item.
 */
final readonly class OrderByItem
{
    public function __construct(
        private Expression $expression,
        private SortDirection $direction = SortDirection::ASC,
        private NullsPosition $nulls = NullsPosition::DEFAULT,
    ) {
    }

    public static function fromAst(SortBy $node) : self
    {
        $nodeExpr = $node->getNode();

        if ($nodeExpr === null) {
            throw InvalidAstException::missingRequiredField('node', 'SortBy');
        }

        $expression = ExpressionFactory::fromAst($nodeExpr);
        $direction = SortDirection::fromProtobuf($node->getSortbyDir());
        $nulls = NullsPosition::fromProtobuf($node->getSortbyNulls());

        return new self($expression, $direction, $nulls);
    }

    public function asc() : self
    {
        return new self($this->expression, SortDirection::ASC, $this->nulls);
    }

    public function desc() : self
    {
        return new self($this->expression, SortDirection::DESC, $this->nulls);
    }

    public function direction() : SortDirection
    {
        return $this->direction;
    }

    public function expression() : Expression
    {
        return $this->expression;
    }

    public function nulls() : NullsPosition
    {
        return $this->nulls;
    }

    public function nullsFirst() : self
    {
        return new self($this->expression, $this->direction, NullsPosition::FIRST);
    }

    public function nullsLast() : self
    {
        return new self($this->expression, $this->direction, NullsPosition::LAST);
    }

    public function toAst() : SortBy
    {
        return new SortBy([
            'node' => $this->expression->toAst(),
            'sortby_dir' => $this->direction->toProtobuf(),
            'sortby_nulls' => $this->nulls->toProtobuf(),
        ]);
    }
}
