<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{A_Expr_Kind, MinMaxOp, Node};
use Flow\PostgreSql\QueryBuilder\Exception\UnsupportedNodeException;

/**
 * Factory for creating Expression instances from AST nodes.
 */
final class ExpressionFactory
{
    public static function fromAst(Node $node) : Expression
    {
        if ($node->getAStar() !== null) {
            return Star::fromAst($node);
        }

        if ($node->getColumnRef() !== null) {
            $columnRef = $node->getColumnRef();
            $fields = $columnRef->getFields();

            if ($fields !== null && \count($fields) > 0) {
                $lastField = $fields[\count($fields) - 1];

                if ($lastField->getAStar() !== null) {
                    return Star::fromAst($node);
                }
            }

            return Column::fromAst($node);
        }

        if ($node->getAConst() !== null) {
            return Literal::fromAst($node);
        }

        if ($node->getFuncCall() !== null) {
            $funcCall = $node->getFuncCall();

            if ($funcCall->getOver() !== null) {
                return WindowFunction::fromAst($node);
            }

            if ($funcCall->getAggStar() || $funcCall->getAggDistinct() || $funcCall->getAggOrder() !== null || $funcCall->getAggFilter() !== null) {
                return AggregateCall::fromAst($node);
            }

            return FunctionCall::fromAst($node);
        }

        if ($node->getTypeCast() !== null) {
            return TypeCast::fromAst($node);
        }

        if ($node->getAExpr() !== null) {
            $aExpr = $node->getAExpr();
            $kind = $aExpr->getKind();

            if ($kind === A_Expr_Kind::AEXPR_NULLIF) {
                return NullIf::fromAst($node);
            }

            return BinaryExpression::fromAst($node);
        }

        if ($node->getCoalesceExpr() !== null) {
            return Coalesce::fromAst($node);
        }

        if ($node->getResTarget() !== null) {
            $resTarget = $node->getResTarget();
            $aliasName = $resTarget->getName();

            // If there's an alias, return an AliasedExpression
            if ($aliasName !== null && $aliasName !== '') {
                return AliasedExpression::fromAst($node);
            }

            // Otherwise, unwrap and return the inner expression
            $valNode = $resTarget->getVal();

            if ($valNode !== null) {
                return self::fromAst($valNode);
            }
        }

        if ($node->getParamRef() !== null) {
            return Parameter::fromAst($node);
        }

        if ($node->getCaseExpr() !== null) {
            return CaseExpression::fromAst($node);
        }

        if ($node->getSubLink() !== null) {
            return Subquery::fromAst($node);
        }

        if ($node->getMinMaxExpr() !== null) {
            $minMaxExpr = $node->getMinMaxExpr();

            if ($minMaxExpr->getOp() === MinMaxOp::IS_GREATEST) {
                return Greatest::fromAst($node);
            }

            return Least::fromAst($node);
        }

        if ($node->getRowExpr() !== null) {
            return RowExpression::fromAst($node);
        }

        if ($node->getAArrayExpr() !== null) {
            return ArrayExpression::fromAst($node);
        }

        throw UnsupportedNodeException::forNodeType('Unknown expression node type');
    }
}
