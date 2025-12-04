<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PgQuery\QueryBuilder\Expression\{AliasedExpression, Parameter};
use PHPUnit\Framework\TestCase;

final class ParameterTest extends TestCase
{
    public function test_converts_to_ast() : void
    {
        $param = Parameter::positional(3);
        $node = $param->toAst();

        $paramRef = $node->getParamRef();
        self::assertNotNull($paramRef);
        self::assertSame(3, $paramRef->getNumber());
    }

    public function test_creates_aliased_expression() : void
    {
        $param = Parameter::positional(1);
        $aliased = $param->as('p');

        self::assertInstanceOf(AliasedExpression::class, $aliased);
        self::assertSame('p', $aliased->getAlias());
        self::assertSame($param, $aliased->getExpression());
    }

    public function test_creates_parameter_with_higher_number() : void
    {
        $param = Parameter::positional(42);

        self::assertSame(42, $param->number());
    }

    public function test_creates_positional_parameter() : void
    {
        $param = Parameter::positional(1);

        self::assertSame(1, $param->number());
    }

    public function test_rejects_negative_parameter() : void
    {
        $this->expectException(InvalidExpressionException::class);

        Parameter::positional(-1);
    }

    public function test_rejects_zero_parameter() : void
    {
        $this->expectException(InvalidExpressionException::class);

        Parameter::positional(0);
    }

    public function test_roundtrip_conversion() : void
    {
        $original = Parameter::positional(5);
        $node = $original->toAst();
        $reconstructed = Parameter::fromAst($node);

        self::assertEquals($original->number(), $reconstructed->number());
    }
}
