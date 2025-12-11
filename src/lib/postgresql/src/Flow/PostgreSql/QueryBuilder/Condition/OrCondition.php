<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\{A_Expr_Kind, SubLinkType};
use Flow\PostgreSql\Protobuf\AST\{BoolExpr, BoolExprType, Node};
use Flow\PostgreSql\QueryBuilder\Exception\{InvalidAstException, UnsupportedNodeException};

final readonly class OrCondition implements Condition
{
    /**
     * @var array<Condition>
     */
    private array $conditions;

    public function __construct(
        Condition ...$conditions,
    ) {
        $this->conditions = $conditions;
    }

    public static function fromAst(Node $node) : static
    {
        if (!$node->hasBoolExpr()) {
            throw InvalidAstException::unexpectedNodeType('BoolExpr', 'unknown');
        }

        $boolExpr = $node->getBoolExpr();

        if ($boolExpr === null) {
            throw InvalidAstException::missingRequiredField('bool_expr', 'Node');
        }

        if ($boolExpr->getBoolop() !== BoolExprType::OR_EXPR) {
            throw InvalidAstException::unexpectedNodeType(
                'BoolExpr with OR_EXPR',
                'BoolExpr with ' . BoolExprType::name($boolExpr->getBoolop())
            );
        }

        $conditions = [];

        foreach ($boolExpr->getArgs() as $argNode) {
            $conditions[] = self::conditionFromNode($argNode);
        }

        return new self(...$conditions);
    }

    public function and(Condition $other) : AndCondition
    {
        return new AndCondition($this, $other);
    }

    public function not() : NotCondition
    {
        return new NotCondition($this);
    }

    public function or(Condition $other) : self
    {
        if ($other instanceof self) {
            return new self(...[...$this->conditions, ...$other->conditions]);
        }

        return new self(...[...$this->conditions, $other]);
    }

    public function toAst() : Node
    {
        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::OR_EXPR);

        $args = [];

        foreach ($this->conditions as $condition) {
            $args[] = $condition->toAst();
        }

        $boolExpr->setArgs($args);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        return $node;
    }

    private static function conditionFromNode(Node $node) : Condition
    {
        if ($node->hasBoolExpr()) {
            $boolExpr = $node->getBoolExpr();

            if ($boolExpr === null) {
                throw InvalidAstException::missingRequiredField('bool_expr', 'Node');
            }

            return match ($boolExpr->getBoolop()) {
                BoolExprType::AND_EXPR => AndCondition::fromAst($node),
                BoolExprType::OR_EXPR => self::fromAst($node),
                BoolExprType::NOT_EXPR => NotCondition::fromAst($node),
                default => throw UnsupportedNodeException::forNodeType('BoolExpr with ' . BoolExprType::name($boolExpr->getBoolop())),
            };
        }

        if ($node->hasAExpr()) {
            $aExpr = $node->getAExpr();

            if ($aExpr === null) {
                throw InvalidAstException::missingRequiredField('a_expr', 'Node');
            }

            return match ($aExpr->getKind()) {
                A_Expr_Kind::AEXPR_OP => Comparison::fromAst($node),
                A_Expr_Kind::AEXPR_LIKE => Like::fromAst($node),
                A_Expr_Kind::AEXPR_ILIKE => Like::fromAst($node),
                A_Expr_Kind::AEXPR_SIMILAR => SimilarTo::fromAst($node),
                A_Expr_Kind::AEXPR_DISTINCT => IsDistinctFrom::fromAst($node),
                A_Expr_Kind::AEXPR_NOT_DISTINCT => IsDistinctFrom::fromAst($node),
                A_Expr_Kind::AEXPR_OP_ANY => Any::fromAst($node),
                A_Expr_Kind::AEXPR_OP_ALL => All::fromAst($node),
                A_Expr_Kind::AEXPR_BETWEEN => Between::fromAst($node),
                A_Expr_Kind::AEXPR_NOT_BETWEEN => Between::fromAst($node),
                A_Expr_Kind::AEXPR_BETWEEN_SYM => Between::fromAst($node),
                A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => Between::fromAst($node),
                default => throw UnsupportedNodeException::forNodeType('A_Expr with kind: ' . A_Expr_Kind::name($aExpr->getKind())),
            };
        }

        if ($node->hasSubLink()) {
            $subLink = $node->getSubLink();

            if ($subLink === null) {
                throw InvalidAstException::missingRequiredField('sub_link', 'Node');
            }

            return match ($subLink->getSubLinkType()) {
                SubLinkType::EXISTS_SUBLINK => Exists::fromAst($node),
                SubLinkType::ANY_SUBLINK => Any::fromAst($node),
                SubLinkType::ALL_SUBLINK => All::fromAst($node),
                default => throw UnsupportedNodeException::forNodeType('SubLink with type: ' . SubLinkType::name($subLink->getSubLinkType())),
            };
        }

        throw UnsupportedNodeException::forNodeType('unknown node type in condition');
    }
}
