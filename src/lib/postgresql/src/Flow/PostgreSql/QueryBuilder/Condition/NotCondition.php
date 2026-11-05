<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\BoolExpr;
use Flow\PostgreSql\Protobuf\AST\BoolExprType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;

use function count;

final readonly class NotCondition implements Condition
{
    public function __construct(
        private Expression $expression,
    ) {}

    public static function fromAst(Node $node): static
    {
        if (!$node->hasBoolExpr()) {
            throw InvalidAstException::unexpectedNodeType('BoolExpr', 'unknown');
        }

        $boolExpr = $node->getBoolExpr();

        if ($boolExpr === null) {
            throw InvalidAstException::missingRequiredField('bool_expr', 'Node');
        }

        if ($boolExpr->getBoolop() !== BoolExprType::NOT_EXPR) {
            throw InvalidAstException::unexpectedNodeType(
                'BoolExpr with NOT_EXPR',
                'BoolExpr with ' . (string) BoolExprType::name($boolExpr->getBoolop()),
            );
        }

        $args = $boolExpr->getArgs();

        if (count($args) !== 1) {
            throw InvalidAstException::invalidFieldValue(
                'args',
                'BoolExpr',
                'NOT_EXPR must have exactly one argument, got ' . count($args),
            );
        }

        return new self(ConditionFactory::fromAst($args[0]));
    }

    public function and(Condition $other): AndCondition
    {
        return new AndCondition($this, $other);
    }

    public function as(string $alias): AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function not(): self
    {
        return new self($this);
    }

    public function or(Condition $other): OrCondition
    {
        return new OrCondition($this, $other);
    }

    public function toAst(): Node
    {
        $boolExpr = new BoolExpr();
        $boolExpr->setBoolop(BoolExprType::NOT_EXPR);
        $boolExpr->setArgs([$this->expression->toAst()]);

        $node = new Node();
        $node->setBoolExpr($boolExpr);

        return $node;
    }
}
