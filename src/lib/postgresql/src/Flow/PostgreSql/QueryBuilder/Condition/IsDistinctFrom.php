<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\{A_Expr, A_Expr_Kind, Node, PBString};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{AliasedExpression, Expression, ExpressionFactory};

final readonly class IsDistinctFrom implements Condition
{
    public function __construct(
        public Expression $left,
        public Expression $right,
        public bool $negated = false,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $aExpr = $node->getAExpr();

        if ($aExpr === null) {
            throw InvalidAstException::unexpectedNodeType('A_Expr', 'unknown');
        }

        $kind = $aExpr->getKind();

        if ($kind !== A_Expr_Kind::AEXPR_DISTINCT && $kind !== A_Expr_Kind::AEXPR_NOT_DISTINCT) {
            throw InvalidAstException::invalidFieldValue('kind', 'A_Expr', 'Expected AEXPR_DISTINCT or AEXPR_NOT_DISTINCT for IsDistinctFrom condition');
        }

        $negated = $kind === A_Expr_Kind::AEXPR_NOT_DISTINCT;

        $lexpr = $aExpr->getLexpr();

        if ($lexpr === null) {
            throw InvalidAstException::missingRequiredField('lexpr', 'A_Expr');
        }

        $rexpr = $aExpr->getRexpr();

        if ($rexpr === null) {
            throw InvalidAstException::missingRequiredField('rexpr', 'A_Expr');
        }

        return new self(
            ExpressionFactory::fromAst($lexpr),
            ExpressionFactory::fromAst($rexpr),
            $negated
        );
    }

    public function and(Condition $other) : AndCondition
    {
        return new AndCondition($this, $other);
    }

    public function as(string $alias) : AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function not() : NotCondition
    {
        return new NotCondition($this);
    }

    public function or(Condition $other) : OrCondition
    {
        return new OrCondition($this, $other);
    }

    public function toAst() : Node
    {
        $kind = $this->negated ? A_Expr_Kind::AEXPR_NOT_DISTINCT : A_Expr_Kind::AEXPR_DISTINCT;

        $operatorString = new PBString(['sval' => '=']);
        $operatorNode = new Node(['string' => $operatorString]);

        $aExpr = new A_Expr([
            'kind' => $kind,
            'name' => [$operatorNode],
            'lexpr' => $this->left->toAst(),
            'rexpr' => $this->right->toAst(),
        ]);

        return new Node(['a_expr' => $aExpr]);
    }
}
