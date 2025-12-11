<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\{A_Expr, A_Expr_Kind, Node, PBString};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{Expression, ExpressionFactory};

/**
 * Generic operator condition for any binary operator (e.g., @>, <@, &&, ~, @@).
 *
 * Unlike Comparison which only supports standard comparison operators (=, <>, <, <=, >, >=),
 * OperatorCondition accepts any valid PostgreSQL operator string.
 */
final readonly class OperatorCondition implements Condition
{
    public function __construct(
        public Expression $left,
        public string $operator,
        public Expression $right,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $aExpr = $node->getAExpr();

        if ($aExpr === null) {
            throw InvalidAstException::unexpectedNodeType('A_Expr', 'unknown');
        }

        if ($aExpr->getKind() !== A_Expr_Kind::AEXPR_OP) {
            throw InvalidAstException::invalidFieldValue('kind', 'A_Expr', 'Expected AEXPR_OP for OperatorCondition');
        }

        $lexpr = $aExpr->getLexpr();
        $rexpr = $aExpr->getRexpr();

        if ($lexpr === null) {
            throw InvalidAstException::missingRequiredField('lexpr', 'A_Expr');
        }

        if ($rexpr === null) {
            throw InvalidAstException::missingRequiredField('rexpr', 'A_Expr');
        }

        $nameNodes = $aExpr->getName();

        if ($nameNodes === null || $nameNodes->count() === 0) {
            throw InvalidAstException::missingRequiredField('name', 'A_Expr');
        }

        $nameNode = $nameNodes->offsetGet(0);
        $stringNode = $nameNode->getString();

        if ($stringNode === null) {
            throw InvalidAstException::unexpectedNodeType('String', 'unknown');
        }

        return new self(
            ExpressionFactory::fromAst($lexpr),
            $stringNode->getSval(),
            ExpressionFactory::fromAst($rexpr)
        );
    }

    public function and(Condition $other) : AndCondition
    {
        return new AndCondition($this, $other);
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
        $operatorString = new PBString(['sval' => $this->operator]);
        $operatorNode = new Node(['string' => $operatorString]);

        $aExpr = new A_Expr([
            'kind' => A_Expr_Kind::AEXPR_OP,
            'name' => [$operatorNode],
            'lexpr' => $this->left->toAst(),
            'rexpr' => $this->right->toAst(),
        ]);

        return new Node(['a_expr' => $aExpr]);
    }
}
