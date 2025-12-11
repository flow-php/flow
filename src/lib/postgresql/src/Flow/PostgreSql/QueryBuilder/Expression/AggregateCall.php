<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{FuncCall, Node, PBString};
use Flow\PostgreSql\QueryBuilder\Clause\OrderBy;
use Flow\PostgreSql\QueryBuilder\Exception\{InvalidAstException, InvalidExpressionException};

/**
 * Represents an aggregate function call with optional DISTINCT, ORDER BY, and FILTER clauses.
 * Examples: COUNT(*), SUM(x), AVG(DISTINCT value), COUNT(*) FILTER (WHERE condition).
 */
final readonly class AggregateCall implements Expression
{
    /**
     * @param non-empty-list<string> $funcName Function name parts (e.g., ['pg_catalog', 'count'] or ['sum'])
     * @param list<Expression> $args Function arguments (empty for COUNT(*))
     * @param bool $star Whether this is COUNT(*) aggregate
     * @param bool $distinct Whether DISTINCT is applied to arguments
     * @param list<OrderBy> $orderBy ORDER BY clauses for aggregates that support ordering
     * @param null|Expression $filter FILTER (WHERE ...) clause
     */
    public function __construct(
        private array $funcName,
        private array $args = [],
        private bool $star = false,
        private bool $distinct = false,
        private array $orderBy = [],
        private ?Expression $filter = null,
    ) {
        if ($this->funcName === []) {
            throw InvalidExpressionException::emptyArray('Function name');
        }

        if ($this->star && $this->args !== []) {
            throw InvalidExpressionException::invalidValue('AggregateCall', 'Cannot have both star and arguments');
        }
    }

    public static function fromAst(Node $node) : static
    {
        $funcCall = $node->getFuncCall();

        if ($funcCall === null) {
            throw InvalidAstException::unexpectedNodeType('FuncCall', 'unknown');
        }

        $funcNameNodes = $funcCall->getFuncname();

        if ($funcNameNodes === null || \count($funcNameNodes) === 0) {
            throw InvalidAstException::missingRequiredField('funcname', 'FuncCall');
        }

        $funcName = [];

        foreach ($funcNameNodes as $nameNode) {
            $stringNode = $nameNode->getString();

            if ($stringNode === null) {
                throw InvalidAstException::invalidFieldValue('funcname', 'FuncCall', 'expected String node');
            }

            $funcName[] = $stringNode->getSval();
        }

        if ($funcName === []) {
            throw InvalidAstException::invalidFieldValue('funcname', 'FuncCall', 'cannot be empty');
        }

        $args = [];
        $argsNodes = $funcCall->getArgs();

        if ($argsNodes !== null) {
            foreach ($argsNodes as $argNode) {
                $args[] = ExpressionFactory::fromAst($argNode);
            }
        }

        $star = $funcCall->getAggStar();
        $distinct = $funcCall->getAggDistinct();

        $orderBy = [];
        $orderByNodes = $funcCall->getAggOrder();

        if ($orderByNodes !== null) {
            foreach ($orderByNodes as $orderByNode) {
                $orderBy[] = OrderBy::fromAst($orderByNode);
            }
        }

        $filter = null;
        $filterNode = $funcCall->getAggFilter();

        if ($filterNode !== null) {
            $filter = ExpressionFactory::fromAst($filterNode);
        }

        return new self($funcName, $args, $star, $distinct, $orderBy, $filter);
    }

    public function as(string $alias) : AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    /**
     * @return list<Expression>
     */
    public function getArgs() : array
    {
        return $this->args;
    }

    public function getFilter() : ?Expression
    {
        return $this->filter;
    }

    /**
     * @return non-empty-list<string>
     */
    public function getFuncName() : array
    {
        return $this->funcName;
    }

    /**
     * @return list<OrderBy>
     */
    public function getOrderBy() : array
    {
        return $this->orderBy;
    }

    public function isDistinct() : bool
    {
        return $this->distinct;
    }

    public function isStar() : bool
    {
        return $this->star;
    }

    public function toAst() : Node
    {
        $funcCall = new FuncCall();
        $funcNameNodes = [];

        foreach ($this->funcName as $namePart) {
            $stringNode = new PBString();
            $stringNode->setSval($namePart);

            $nameNode = new Node();
            $nameNode->setString($stringNode);

            $funcNameNodes[] = $nameNode;
        }

        $funcCall->setFuncname($funcNameNodes);

        $argNodes = [];

        foreach ($this->args as $arg) {
            $argNodes[] = $arg->toAst();
        }

        $funcCall->setArgs($argNodes);

        if ($this->star) {
            $funcCall->setAggStar(true);
        }

        if ($this->distinct) {
            $funcCall->setAggDistinct(true);
        }

        $orderByNodes = [];

        foreach ($this->orderBy as $orderByClause) {
            $orderByNodes[] = $orderByClause->toAst();
        }

        if ($orderByNodes !== []) {
            $funcCall->setAggOrder($orderByNodes);
        }

        if ($this->filter !== null) {
            $funcCall->setAggFilter($this->filter->toAst());
        }

        $node = new Node();
        $node->setFuncCall($funcCall);

        return $node;
    }

    public function withDistinct(bool $distinct = true) : self
    {
        return new self($this->funcName, $this->args, $this->star, $distinct, $this->orderBy, $this->filter);
    }

    public function withFilter(Expression $filter) : self
    {
        return new self($this->funcName, $this->args, $this->star, $this->distinct, $this->orderBy, $filter);
    }

    public function withOrderBy(OrderBy ...$orderBy) : self
    {
        return new self($this->funcName, $this->args, $this->star, $this->distinct, \array_values($orderBy), $this->filter);
    }
}
