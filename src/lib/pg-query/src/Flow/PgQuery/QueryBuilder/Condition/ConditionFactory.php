<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Condition;

use Flow\PgQuery\Protobuf\AST\{A_Expr_Kind, BoolExprType, Node, SubLinkType};
use Flow\PgQuery\QueryBuilder\Exception\UnsupportedNodeException;

/**
 * Factory for creating Condition instances from AST nodes.
 */
final class ConditionFactory
{
    public static function fromAst(Node $node) : Condition
    {
        if ($node->getAExpr() !== null) {
            $aExpr = $node->getAExpr();
            $kind = $aExpr->getKind();

            return match ($kind) {
                A_Expr_Kind::AEXPR_OP => Comparison::fromAst($node),
                A_Expr_Kind::AEXPR_LIKE => Like::fromAst($node),
                A_Expr_Kind::AEXPR_ILIKE => Like::fromAst($node),
                A_Expr_Kind::AEXPR_SIMILAR => SimilarTo::fromAst($node),
                A_Expr_Kind::AEXPR_BETWEEN => Between::fromAst($node),
                A_Expr_Kind::AEXPR_NOT_BETWEEN => Between::fromAst($node),
                A_Expr_Kind::AEXPR_IN => In::fromAst($node),
                A_Expr_Kind::AEXPR_OP_ANY => Any::fromAst($node),
                A_Expr_Kind::AEXPR_OP_ALL => All::fromAst($node),
                A_Expr_Kind::AEXPR_DISTINCT => IsDistinctFrom::fromAst($node),
                A_Expr_Kind::AEXPR_NOT_DISTINCT => IsDistinctFrom::fromAst($node),
                default => throw UnsupportedNodeException::forNodeType("A_Expr kind: {$kind}"),
            };
        }

        if ($node->getBoolExpr() !== null) {
            $boolExpr = $node->getBoolExpr();
            $boolOp = $boolExpr->getBoolop();

            return match ($boolOp) {
                BoolExprType::AND_EXPR => AndCondition::fromAst($node),
                BoolExprType::OR_EXPR => OrCondition::fromAst($node),
                BoolExprType::NOT_EXPR => NotCondition::fromAst($node),
                default => throw UnsupportedNodeException::forNodeType("BoolExpr type: {$boolOp}"),
            };
        }

        if ($node->getNullTest() !== null) {
            return IsNull::fromAst($node);
        }

        if ($node->getSubLink() !== null) {
            $subLink = $node->getSubLink();
            $subLinkType = $subLink->getSubLinkType();

            return match ($subLinkType) {
                SubLinkType::EXISTS_SUBLINK => Exists::fromAst($node),
                SubLinkType::ANY_SUBLINK => Any::fromAst($node),
                SubLinkType::ALL_SUBLINK => All::fromAst($node),
                default => throw UnsupportedNodeException::forNodeType("SubLink type: {$subLinkType}"),
            };
        }

        throw UnsupportedNodeException::forNodeType('Unknown condition node type');
    }
}
