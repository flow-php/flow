<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\CoalesceExpr;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

/**
 * COALESCE(expr, expr, ...) - returns first non-null expression.
 */
final readonly class Coalesce implements Expression
{
    /**
     * @param array<Expression> $expressions
     */
    public function __construct(
        private array $expressions,
    ) {
        if (\count($this->expressions) < 2) {
            throw new \InvalidArgumentException('COALESCE requires at least 2 expressions');
        }
    }

    public static function fromAst(Node $node): static
    {
        $coalesceExpr = $node->getCoalesceExpr();

        if ($coalesceExpr === null) {
            throw InvalidAstException::unexpectedNodeType('CoalesceExpr', 'unknown');
        }

        $args = $coalesceExpr->getArgs();

        if (\count($args) < 2) {
            throw InvalidAstException::invalidFieldValue('args', 'CoalesceExpr', 'must have at least 2 arguments');
        }

        $expressions = [];

        foreach ($args as $argNode) {
            $expressions[] = ExpressionFactory::fromAst($argNode);
        }

        return new self($expressions);
    }

    public function as(string $alias): AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    /**
     * @return array<Expression>
     */
    public function expressions(): array
    {
        return $this->expressions;
    }

    public function toAst(): Node
    {
        $args = [];

        foreach ($this->expressions as $expression) {
            $args[] = $expression->toAst();
        }

        $coalesceExpr = new CoalesceExpr();
        $coalesceExpr->setArgs($args);
        $coalesceExpr->setCoalescetype(0);
        $coalesceExpr->setCoalescecollid(0);
        $coalesceExpr->setLocation(-1);

        $node = new Node();
        $node->setCoalesceExpr($coalesceExpr);

        return $node;
    }
}
