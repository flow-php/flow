<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{FuncCall, Node, PBString, WindowDef};
use Flow\PostgreSql\QueryBuilder\Clause\{OrderBy, OrderByItem};
use Flow\PostgreSql\QueryBuilder\Exception\{InvalidAstException, InvalidExpressionException};

final readonly class WindowFunction implements Expression
{
    /**
     * @param non-empty-list<string> $funcName Function name parts (e.g., ['row_number'] or ['rank'])
     * @param list<Expression> $args Function arguments
     * @param list<Expression> $partitionBy PARTITION BY expressions
     * @param list<OrderBy|OrderByItem> $orderBy ORDER BY clauses
     */
    public function __construct(
        private array $funcName,
        private array $args = [],
        private array $partitionBy = [],
        private array $orderBy = [],
    ) {
        if ($this->funcName === []) {
            throw InvalidExpressionException::emptyArray('Function name');
        }
    }

    public static function fromAst(Node $node) : static
    {
        $funcCall = $node->getFuncCall();

        if ($funcCall === null) {
            throw InvalidAstException::unexpectedNodeType('FuncCall', 'unknown');
        }

        $overNode = $funcCall->getOver();

        if ($overNode === null) {
            throw InvalidAstException::missingRequiredField('over', 'FuncCall');
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

        $partitionBy = [];
        $partitionNodes = $overNode->getPartitionClause();

        if ($partitionNodes !== null) {
            foreach ($partitionNodes as $partitionNode) {
                $partitionBy[] = ExpressionFactory::fromAst($partitionNode);
            }
        }

        $orderBy = [];
        $orderNodes = $overNode->getOrderClause();

        if ($orderNodes !== null) {
            foreach ($orderNodes as $orderNode) {
                $orderBy[] = OrderBy::fromAst($orderNode);
            }
        }

        return new self($funcName, $args, $partitionBy, $orderBy);
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

    /**
     * @return non-empty-list<string>
     */
    public function getFuncName() : array
    {
        return $this->funcName;
    }

    /**
     * @return list<OrderBy|OrderByItem>
     */
    public function getOrderBy() : array
    {
        return $this->orderBy;
    }

    /**
     * @return list<Expression>
     */
    public function getPartitionBy() : array
    {
        return $this->partitionBy;
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

        $windowDef = new WindowDef();

        $partitionNodes = [];

        foreach ($this->partitionBy as $partition) {
            $partitionNodes[] = $partition->toAst();
        }

        if ($partitionNodes !== []) {
            $windowDef->setPartitionClause($partitionNodes);
        }

        $orderNodes = [];

        foreach ($this->orderBy as $order) {
            if ($order instanceof OrderByItem) {
                $orderNodes[] = new Node(['sort_by' => $order->toAst()]);
            } else {
                $orderNodes[] = $order->toAst();
            }
        }

        if ($orderNodes !== []) {
            $windowDef->setOrderClause($orderNodes);
        }

        $funcCall->setOver($windowDef);

        $node = new Node();
        $node->setFuncCall($funcCall);

        return $node;
    }

    public function withArgs(Expression ...$args) : self
    {
        return new self($this->funcName, \array_values($args), $this->partitionBy, $this->orderBy);
    }

    public function withOrderBy(OrderBy ...$orderBy) : self
    {
        return new self($this->funcName, $this->args, $this->partitionBy, \array_values($orderBy));
    }

    public function withPartitionBy(Expression ...$partitionBy) : self
    {
        return new self($this->funcName, $this->args, \array_values($partitionBy), $this->orderBy);
    }
}
