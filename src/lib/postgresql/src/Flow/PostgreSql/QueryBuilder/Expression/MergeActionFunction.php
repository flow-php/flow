<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\MergeSupportFunc;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;

final readonly class MergeActionFunction implements Expression
{
    public static function fromAst(Node $node): static
    {
        if ($node->getMergeSupportFunc() === null) {
            throw InvalidAstException::unexpectedNodeType('MergeSupportFunc', 'unknown');
        }

        return new self();
    }

    public function as(string $alias): AliasedExpression
    {
        return AliasedExpression::create($this, $alias);
    }

    public function toAst(): Node
    {
        $node = new Node();
        $node->setMergeSupportFunc(new MergeSupportFunc());

        return $node;
    }
}
