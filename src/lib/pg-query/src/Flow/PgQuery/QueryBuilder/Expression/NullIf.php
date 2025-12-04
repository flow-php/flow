<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{Node, NullIfExpr};
use Flow\PgQuery\QueryBuilder\Exception\{InvalidAstException, UnsupportedNodeException};

/**
 * NULLIF(expr1, expr2) - returns NULL if expr1 equals expr2, otherwise returns expr1.
 */
final readonly class NullIf implements Expression
{
    public function __construct(
        private Expression $first,
        private Expression $second,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $nullIfExpr = $node->getNullIfExpr();

        if ($nullIfExpr === null) {
            throw InvalidAstException::unexpectedNodeType('NullIfExpr', 'unknown');
        }

        $args = $nullIfExpr->getArgs();

        if ($args === null || \count($args) !== 2) {
            throw InvalidAstException::invalidFieldValue('args', 'NullIfExpr', 'must have exactly 2 arguments');
        }

        return new self(
            self::expressionFromNode($args[0]),
            self::expressionFromNode($args[1])
        );
    }

    public function as(string $alias) : AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function first() : Expression
    {
        return $this->first;
    }

    public function second() : Expression
    {
        return $this->second;
    }

    public function toAst() : Node
    {
        $nullIfExpr = new NullIfExpr();
        $nullIfExpr->setArgs([
            $this->first->toAst(),
            $this->second->toAst(),
        ]);
        $nullIfExpr->setOpno(0);
        $nullIfExpr->setOpresulttype(0);
        $nullIfExpr->setOpretset(false);
        $nullIfExpr->setOpcollid(0);
        $nullIfExpr->setInputcollid(0);
        $nullIfExpr->setLocation(-1);

        $node = new Node();
        $node->setNullIfExpr($nullIfExpr);

        return $node;
    }

    private static function expressionFromNode(Node $node) : Expression
    {
        throw UnsupportedNodeException::cannotReconstruct('Expression from arbitrary Node - implement Expression::fromAst() factory');
    }
}
