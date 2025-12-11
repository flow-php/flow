<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{A_Const, Node, PBString};
use Flow\PostgreSql\QueryBuilder\Expression\{AliasedExpression, Expression};

/**
 * Mock expression for testing - represents a simple string literal.
 */
final readonly class MockExpression implements Expression
{
    public function __construct(
        private string $value = 'mock_value',
    ) {
    }

    public static function fromAst(Node $node) : static
    {
        $aConst = $node->getAConst();

        if ($aConst === null) {
            throw new \RuntimeException('Expected A_Const node');
        }

        $sval = $aConst->getSval();

        if ($sval === null) {
            throw new \RuntimeException('Expected sval in A_Const');
        }

        return new self($sval->getSval());
    }

    public function as(string $alias) : AliasedExpression
    {
        return new AliasedExpression($this, $alias);
    }

    public function toAst() : Node
    {
        $sval = new PBString();
        $sval->setSval($this->value);

        $aConst = new A_Const();
        $aConst->setSval($sval);
        $aConst->setLocation(-1);

        $node = new Node();
        $node->setAConst($aConst);

        return $node;
    }

    public function value() : string
    {
        return $this->value;
    }
}
