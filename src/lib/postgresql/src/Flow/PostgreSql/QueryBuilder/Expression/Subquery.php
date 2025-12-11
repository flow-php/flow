<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{Node, SubLink, SubLinkType};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

final readonly class Subquery implements Expression
{
    public function __construct(
        private Node $selectStatement,
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $subLink = $node->getSubLink();

        if ($subLink === null) {
            throw InvalidAstException::unexpectedNodeType('SubLink', 'unknown');
        }

        if ($subLink->getSubLinkType() !== SubLinkType::EXPR_SUBLINK) {
            throw InvalidAstException::invalidFieldValue(
                'subLinkType',
                'SubLink',
                \sprintf('expected EXPR_SUBLINK (%d), got %d', SubLinkType::EXPR_SUBLINK, $subLink->getSubLinkType())
            );
        }

        $subselectNode = $subLink->getSubselect();

        if ($subselectNode === null) {
            throw InvalidAstException::missingRequiredField('subselect', 'SubLink');
        }

        return new self($subselectNode);
    }

    public function as(string $alias) : AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function getSelectStatement() : Node
    {
        return $this->selectStatement;
    }

    public function toAst() : Node
    {
        $subLink = new SubLink();
        $subLink->setSubLinkType(SubLinkType::EXPR_SUBLINK);
        $subLink->setSubselect($this->selectStatement);

        $node = new Node();
        $node->setSubLink($subLink);

        return $node;
    }
}
