<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\BoolExpr;
use Flow\PostgreSql\Protobuf\AST\BoolExprType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;

final readonly class OrCondition implements Condition
{
    /**
     * @var array<Condition>
     */
    private array $conditions;

    public function __construct(Condition ...$conditions)
    {
        $this->conditions = $conditions;
    }

    public static function fromAst(Node $node): static
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
                'BoolExpr with ' . (string) BoolExprType::name($boolExpr->getBoolop()),
            );
        }

        $conditions = [];

        foreach ($boolExpr->getArgs() as $argNode) {
            $conditions[] = ConditionFactory::fromAst($argNode);
        }

        return new self(...$conditions);
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

    public function or(Condition $other): self
    {
        if ($other instanceof self) {
            return new self(...[...$this->conditions, ...$other->conditions]);
        }

        return new self(...[...$this->conditions, $other]);
    }

    public function toAst(): Node
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
}
