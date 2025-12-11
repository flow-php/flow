<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\{Node, SortBy};
use Flow\PostgreSql\QueryBuilder\Bridge\AstConvertible;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{Expression, ExpressionFactory};

/**
 * Represents an ORDER BY clause element.
 */
final readonly class OrderBy implements AstConvertible
{
    public function __construct(
        private Expression $expression,
        private SortDirection $direction = SortDirection::ASC,
        private NullsPosition $nullsPosition = NullsPosition::DEFAULT,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $sortBy = $node->getSortBy();

        if ($sortBy === null) {
            throw InvalidAstException::unexpectedNodeType('SortBy', 'unknown');
        }

        $nodeExpr = $sortBy->getNode();

        if ($nodeExpr === null) {
            throw InvalidAstException::missingRequiredField('node', 'SortBy');
        }

        $expression = ExpressionFactory::fromAst($nodeExpr);

        $direction = SortDirection::fromProtobuf($sortBy->getSortbyDir());
        $nullsPosition = NullsPosition::fromProtobuf($sortBy->getSortbyNulls());

        return new self($expression, $direction, $nullsPosition);
    }

    public function getDirection() : SortDirection
    {
        return $this->direction;
    }

    public function getExpression() : Expression
    {
        return $this->expression;
    }

    public function getNullsPosition() : NullsPosition
    {
        return $this->nullsPosition;
    }

    public function toAst() : Node
    {
        $sortBy = new SortBy([
            'node' => $this->expression->toAst(),
            'sortby_dir' => $this->direction->toProtobuf(),
            'sortby_nulls' => $this->nullsPosition->toProtobuf(),
        ]);

        return new Node(['sort_by' => $sortBy]);
    }

    public function withDirection(SortDirection $direction) : self
    {
        return new self($this->expression, $direction, $this->nullsPosition);
    }

    public function withNullsPosition(NullsPosition $nullsPosition) : self
    {
        return new self($this->expression, $this->direction, $nullsPosition);
    }
}
