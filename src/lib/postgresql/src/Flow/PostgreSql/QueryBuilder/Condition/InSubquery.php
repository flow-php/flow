<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\SubLink;
use Flow\PostgreSql\Protobuf\AST\SubLinkType;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;

final readonly class InSubquery implements Condition
{
    public function __construct(
        public Expression $expression,
        public Node $subquery,
    ) {}

    public static function fromAst(Node $node): static
    {
        $subLink = $node->getSubLink();

        if ($subLink === null) {
            throw InvalidAstException::unexpectedNodeType('SubLink', 'unknown');
        }

        if ($subLink->getSubLinkType() !== SubLinkType::ANY_SUBLINK || $subLink->getOperName()->count() !== 0) {
            throw InvalidAstException::invalidFieldValue(
                'sub_link_type',
                'SubLink',
                'Expected ANY_SUBLINK without oper_name for InSubquery condition',
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

        return new self(ExpressionFactory::fromAst($testexpr), $subselect);
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
        $subLink = new SubLink([
            'sub_link_type' => SubLinkType::ANY_SUBLINK,
            'testexpr' => $this->expression->toAst(),
            'subselect' => $this->subquery,
        ]);

        return new Node(['sub_link' => $subLink]);
    }
}
