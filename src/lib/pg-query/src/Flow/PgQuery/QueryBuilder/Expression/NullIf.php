<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{A_Expr, A_Expr_Kind, Node, PBString};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;

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
        $aExpr = $node->getAExpr();

        if ($aExpr === null) {
            throw InvalidAstException::unexpectedNodeType('A_Expr', 'unknown');
        }

        if ($aExpr->getKind() !== A_Expr_Kind::AEXPR_NULLIF) {
            throw InvalidAstException::invalidFieldValue('kind', 'A_Expr', 'Expected AEXPR_NULLIF for NullIf expression');
        }

        $lexpr = $aExpr->getLexpr();
        $rexpr = $aExpr->getRexpr();

        if ($lexpr === null || $rexpr === null) {
            throw InvalidAstException::missingRequiredField('lexpr/rexpr', 'A_Expr');
        }

        return new self(
            ExpressionFactory::fromAst($lexpr),
            ExpressionFactory::fromAst($rexpr)
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
        $nameString = new PBString();
        $nameString->setSval('=');
        $nameNode = new Node(['string' => $nameString]);

        $aExpr = new A_Expr([
            'kind' => A_Expr_Kind::AEXPR_NULLIF,
            'name' => [$nameNode],
            'lexpr' => $this->first->toAst(),
            'rexpr' => $this->second->toAst(),
        ]);

        return new Node(['a_expr' => $aExpr]);
    }
}
