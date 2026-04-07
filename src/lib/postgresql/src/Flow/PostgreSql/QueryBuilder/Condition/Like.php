<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\{A_Expr, A_Expr_Kind, BoolExpr, BoolExprType, Node, PBString};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{AliasedExpression, Expression, ExpressionFactory};

final readonly class Like implements Condition
{
    public function __construct(
        public Expression $expression,
        public Expression $pattern,
        public bool $caseInsensitive = false,
        public ?Expression $escape = null,
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

        if ($kind !== A_Expr_Kind::AEXPR_LIKE && $kind !== A_Expr_Kind::AEXPR_ILIKE) {
            throw InvalidAstException::invalidFieldValue('kind', 'A_Expr', 'Expected AEXPR_LIKE or AEXPR_ILIKE for Like condition');
        }

        $caseInsensitive = $kind === A_Expr_Kind::AEXPR_ILIKE;

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
            $caseInsensitive,
            null
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
        $kind = $this->caseInsensitive ? A_Expr_Kind::AEXPR_ILIKE : A_Expr_Kind::AEXPR_LIKE;

        $operatorString = new PBString(['sval' => $this->caseInsensitive ? '~~*' : '~~']);
        $operatorNode = new Node(['string' => $operatorString]);

        $aExpr = new A_Expr([
            'kind' => $kind,
            'name' => [$operatorNode],
            'lexpr' => $this->expression->toAst(),
            'rexpr' => $this->pattern->toAst(),
        ]);

        $likeNode = new Node(['a_expr' => $aExpr]);

        if ($this->negated) {
            $boolExpr = new BoolExpr();
            $boolExpr->setBoolop(BoolExprType::NOT_EXPR);
            $boolExpr->setArgs([$likeNode]);

            return new Node(['bool_expr' => $boolExpr]);
        }

        return $likeNode;
    }
}
