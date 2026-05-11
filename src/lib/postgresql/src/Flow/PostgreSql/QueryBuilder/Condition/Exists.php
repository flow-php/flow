<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\SubLink;
use Flow\PostgreSql\Protobuf\AST\SubLinkType;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;

final readonly class Exists implements Condition
{
    public function __construct(
        public Node $subquery,
    ) {}

    public static function fromAst(Node $node): static
    {
        $subLink = $node->getSubLink();

        if ($subLink === null) {
            throw InvalidAstException::unexpectedNodeType('SubLink', 'unknown');
        }

        if ($subLink->getSubLinkType() !== SubLinkType::EXISTS_SUBLINK) {
            throw InvalidAstException::invalidFieldValue(
                'sub_link_type',
                'SubLink',
                'Expected EXISTS_SUBLINK for Exists condition',
            );
        }

        $subselect = $subLink->getSubselect();

        if ($subselect === null) {
            throw InvalidAstException::missingRequiredField('subselect', 'SubLink');
        }

        return new self($subselect);
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
            'sub_link_type' => SubLinkType::EXISTS_SUBLINK,
            'subselect' => $this->subquery,
        ]);

        return new Node(['sub_link' => $subLink]);
    }
}
