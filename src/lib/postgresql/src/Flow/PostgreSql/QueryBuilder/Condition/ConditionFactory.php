<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\BoolExpr;
use Flow\PostgreSql\Protobuf\AST\BoolExprType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\SubLink;
use Flow\PostgreSql\Protobuf\AST\SubLinkType;
use Flow\PostgreSql\QueryBuilder\Exception\UnsupportedNodeException;

use function in_array;

/**
 * Factory for creating Condition instances from AST nodes.
 */
final class ConditionFactory
{
    public static function fromAst(Node $node): Condition
    {
        $aExpr = $node->getAExpr();

        if ($aExpr instanceof A_Expr) {
            $kind = $aExpr->getKind();

            return match ($kind) {
                A_Expr_Kind::AEXPR_OP => self::parseOperatorCondition($node),
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

        $boolExpr = $node->getBoolExpr();

        if ($boolExpr instanceof BoolExpr) {
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

        $subLink = $node->getSubLink();

        if ($subLink instanceof SubLink) {
            $subLinkType = $subLink->getSubLinkType();

            return match ($subLinkType) {
                SubLinkType::EXISTS_SUBLINK => Exists::fromAst($node),
                SubLinkType::ANY_SUBLINK => Any::fromAst($node),
                SubLinkType::ALL_SUBLINK => All::fromAst($node),
                default => throw UnsupportedNodeException::forNodeType("SubLink type: {$subLinkType}"),
            };
        }

        if ($node->getAConst() !== null || $node->getColumnRef() !== null) {
            return BooleanCondition::fromAst($node);
        }

        throw UnsupportedNodeException::forNodeType('Unknown condition node type');
    }

    /**
     * Parse an AEXPR_OP node, trying Comparison first for standard operators,
     * falling back to OperatorCondition for other operators.
     */
    private static function parseOperatorCondition(Node $node): Condition
    {
        $aExpr = $node->getAExpr();

        if ($aExpr === null) {
            return OperatorCondition::fromAst($node);
        }

        $nameNodes = $aExpr->getName();

        if ($nameNodes->count() > 0) {
            $nameNode = $nameNodes->offsetGet(0);
            $stringNode = $nameNode->getString();

            if ($stringNode !== null) {
                $operator = $stringNode->getSval();

                if (in_array($operator, ['=', '<>', '<', '<=', '>', '>='], true)) {
                    return Comparison::fromAst($node);
                }
            }
        }

        return OperatorCondition::fromAst($node);
    }
}
