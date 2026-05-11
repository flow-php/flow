<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\SubLink;
use Flow\PostgreSql\Protobuf\AST\SubLinkType;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;

final readonly class All implements Condition
{
    public function __construct(
        public Expression $expression,
        public ComparisonOperator $operator,
        public Expression|Node $arrayOrSubquery,
    ) {}

    public static function fromAst(Node $node): static
    {
        if ($node->hasSubLink()) {
            $subLink = $node->getSubLink();

            if ($subLink === null) {
                throw InvalidAstException::unexpectedNodeType('SubLink', 'unknown');
            }

            if ($subLink->getSubLinkType() !== SubLinkType::ALL_SUBLINK) {
                throw InvalidAstException::invalidFieldValue(
                    'sub_link_type',
                    'SubLink',
                    'Expected ALL_SUBLINK for All condition',
                );
            }

            $testexpr = $subLink->getTestexpr();

            if ($testexpr === null) {
                throw InvalidAstException::missingRequiredField('testexpr', 'SubLink');
            }

            $subselect = $subLink->getSubselect();

            if ($subselect === null) {
                throw InvalidAstException::missingRequiredField('subselect', 'SubLink');
            }

            $operNames = $subLink->getOperName();

            if ($operNames === null || $operNames->count() === 0) {
                throw InvalidAstException::missingRequiredField('oper_name', 'SubLink');
            }

            $operNode = $operNames->offsetGet(0);
            $stringNode = $operNode->getString();

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
                default => throw InvalidAstException::invalidFieldValue(
                    'oper_name',
                    'SubLink',
                    "Unsupported comparison operator: {$operatorString}",
                ),
            };

            return new self(ExpressionFactory::fromAst($testexpr), $operator, $subselect);
        }

        if ($node->hasAExpr()) {
            $aExpr = $node->getAExpr();

            if ($aExpr === null) {
                throw InvalidAstException::unexpectedNodeType('A_Expr', 'unknown');
            }

            if ($aExpr->getKind() !== A_Expr_Kind::AEXPR_OP_ALL) {
                throw InvalidAstException::invalidFieldValue(
                    'kind',
                    'A_Expr',
                    'Expected AEXPR_OP_ALL for All condition',
                );
            }

            $lexpr = $aExpr->getLexpr();

            if ($lexpr === null) {
                throw InvalidAstException::missingRequiredField('lexpr', 'A_Expr');
            }

            $rexpr = $aExpr->getRexpr();

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
                default => throw InvalidAstException::invalidFieldValue(
                    'name',
                    'A_Expr',
                    "Unsupported comparison operator: {$operatorString}",
                ),
            };

            return new self(ExpressionFactory::fromAst($lexpr), $operator, ExpressionFactory::fromAst($rexpr));
        }

        throw InvalidAstException::unexpectedNodeType('SubLink or A_Expr', 'unknown');
    }

    public function and(Condition $other): AndCondition
    {
        return new AndCondition($this, $other);
    }

    public function as(string $alias): AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function not(): NotCondition
    {
        return new NotCondition($this);
    }

    public function or(Condition $other): OrCondition
    {
        return new OrCondition($this, $other);
    }

    public function toAst(): Node
    {
        if ($this->arrayOrSubquery instanceof Node) {
            $subLink = new SubLink([
                'sub_link_type' => SubLinkType::ALL_SUBLINK,
                'subselect' => $this->arrayOrSubquery,
                'testexpr' => $this->expression->toAst(),
                'oper_name' => [
                    new Node(['string' => new PBString(['sval' => $this->operator->value])]),
                ],
            ]);

            return new Node(['sub_link' => $subLink]);
        }

        $operatorString = new PBString(['sval' => $this->operator->value]);
        $operatorNode = new Node(['string' => $operatorString]);

        $aExpr = new A_Expr([
            'kind' => A_Expr_Kind::AEXPR_OP_ALL,
            'name' => [$operatorNode],
            'lexpr' => $this->expression->toAst(),
            'rexpr' => $this->arrayOrSubquery->toAst(),
        ]);

        return new Node(['a_expr' => $aExpr]);
    }
}
