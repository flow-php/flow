<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{MinMaxExpr, MinMaxOp, Node};
use Flow\PgQuery\QueryBuilder\Exception\{InvalidAstException, UnsupportedNodeException};

/**
 * GREATEST(expr, expr, ...) - returns largest value from list of expressions.
 */
final readonly class Greatest implements Expression
{
    /**
     * @param array<Expression> $expressions
     */
    public function __construct(
        private array $expressions,
    ) {
        if (\count($this->expressions) < 2) {
            throw new \InvalidArgumentException('GREATEST requires at least 2 expressions');
        }
    }

    public static function fromAst(Node $node) : static
    {
        $minMaxExpr = $node->getMinMaxExpr();

        if ($minMaxExpr === null) {
            throw InvalidAstException::unexpectedNodeType('MinMaxExpr', 'unknown');
        }

        if ($minMaxExpr->getOp() !== MinMaxOp::IS_GREATEST) {
            throw InvalidAstException::invalidFieldValue('op', 'MinMaxExpr', 'must be IS_GREATEST');
        }

        $args = $minMaxExpr->getArgs();

        if ($args === null || \count($args) < 2) {
            throw InvalidAstException::invalidFieldValue('args', 'MinMaxExpr', 'must have at least 2 arguments');
        }

        $expressions = [];

        foreach ($args as $argNode) {
            $expressions[] = self::expressionFromNode($argNode);
        }

        return new self($expressions);
    }

    public function as(string $alias) : AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    /**
     * @return array<Expression>
     */
    public function expressions() : array
    {
        return $this->expressions;
    }

    public function toAst() : Node
    {
        $args = [];

        foreach ($this->expressions as $expression) {
            $args[] = $expression->toAst();
        }

        $minMaxExpr = new MinMaxExpr();
        $minMaxExpr->setOp(MinMaxOp::IS_GREATEST);
        $minMaxExpr->setArgs($args);
        $minMaxExpr->setMinmaxtype(0);
        $minMaxExpr->setMinmaxcollid(0);
        $minMaxExpr->setInputcollid(0);
        $minMaxExpr->setLocation(-1);

        $node = new Node();
        $node->setMinMaxExpr($minMaxExpr);

        return $node;
    }

    private static function expressionFromNode(Node $node) : Expression
    {
        throw UnsupportedNodeException::cannotReconstruct('Expression from arbitrary Node - implement Expression::fromAst() factory');
    }
}
