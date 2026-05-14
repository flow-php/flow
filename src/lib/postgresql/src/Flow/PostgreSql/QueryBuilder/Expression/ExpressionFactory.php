<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\FuncCall;
use Flow\PostgreSql\Protobuf\AST\MinMaxOp;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\UnsupportedNodeException;

/**
 * Factory for creating Expression instances from AST nodes.
 */
final class ExpressionFactory
{
    public static function fromAst(Node $node): Expression
    {
        if ($node->getAStar() !== null) {
            return Star::fromAst($node);
        }

        $columnRef = $node->getColumnRef();

        if ($columnRef !== null) {
            $fields = $columnRef->getFields();

            if (\count($fields) > 0) {
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

        $funcCall = $node->getFuncCall();

        if ($funcCall !== null) {
            if ($funcCall->getOver() !== null) {
                return WindowFunction::fromAst($node);
            }

            if (self::isAggregateCall($funcCall)) {
                return AggregateCall::fromAst($node);
            }

            return FunctionCall::fromAst($node);
        }

        if ($node->getTypeCast() !== null) {
            return TypeCast::fromAst($node);
        }

        $aExpr = $node->getAExpr();

        if ($aExpr !== null) {
            if ($aExpr->getKind() === A_Expr_Kind::AEXPR_NULLIF) {
                return NullIf::fromAst($node);
            }

            return BinaryExpression::fromAst($node);
        }

        if ($node->getCoalesceExpr() !== null) {
            return Coalesce::fromAst($node);
        }

        $resTarget = $node->getResTarget();

        if ($resTarget !== null) {
            $aliasName = $resTarget->getName();

            if ($aliasName !== '') {
                return AliasedExpression::fromAst($node);
            }

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

        $minMaxExpr = $node->getMinMaxExpr();

        if ($minMaxExpr !== null) {
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

        if ($node->getSqlValueFunction() !== null) {
            return SQLValueFunctionExpression::fromAst($node);
        }

        throw UnsupportedNodeException::forNodeType('Unknown expression node type');
    }

    private static function isAggregateCall(FuncCall $funcCall): bool
    {
        return (
            $funcCall->getAggStar()
            || $funcCall->getAggDistinct()
            || \count($funcCall->getAggOrder()) > 0
            || $funcCall->getAggFilter() !== null
        );
    }
}
