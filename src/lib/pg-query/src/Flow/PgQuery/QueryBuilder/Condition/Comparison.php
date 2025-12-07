<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Condition;

use Flow\PgQuery\Protobuf\AST\{A_Expr, A_Expr_Kind, Node, PBString};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Expression\{Expression, ExpressionFactory};

final readonly class Comparison implements Condition
{
    public function __construct(
        public Expression $left,
        public ComparisonOperator $operator,
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
            throw InvalidAstException::invalidFieldValue('kind', 'A_Expr', 'Expected AEXPR_OP for Comparison');
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

        $operatorString = $stringNode->getSval();

        $operator = match ($operatorString) {
            '=' => ComparisonOperator::EQ,
            '<>' => ComparisonOperator::NEQ,
            '<' => ComparisonOperator::LT,
            '<=' => ComparisonOperator::LTE,
            '>' => ComparisonOperator::GT,
            '>=' => ComparisonOperator::GTE,
            default => throw InvalidAstException::invalidFieldValue('name', 'A_Expr', "Unsupported comparison operator: {$operatorString}"),
        };

        return new self(
            ExpressionFactory::fromAst($lexpr),
            $operator,
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
        $operatorString = new PBString(['sval' => $this->operator->value]);
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
