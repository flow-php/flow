<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\Boolean;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;

/**
 * Wraps an Expression as a Condition for use in WHERE/HAVING/JOIN ON clauses.
 *
 * Used for boolean columns (e.g., WHERE is_active) and boolean literals (e.g., WHERE true).
 * The AST output is identical to the wrapped expression — this is a type-system bridge only.
 */
final readonly class BooleanCondition implements Condition
{
    public function __construct(
        public Expression $expression,
    ) {}

    public static function fromAst(Node $node): static
    {
        if ($node->hasColumnRef()) {
            return new self(Column::fromAst($node));
        }

        if ($node->hasAConst()) {
            $aConst = $node->getAConst();

            if ($aConst !== null && $aConst->hasBoolval()) {
                $boolval = $aConst->getBoolval();
                \assert($boolval instanceof Boolean);

                return new self(Literal::bool($boolval->getBoolval()));
            }
        }

        return new self(ExpressionFactory::fromAst($node));
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
        return $this->expression->toAst();
    }
}
