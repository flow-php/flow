<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Parameter;
use PHPUnit\Framework\TestCase;

final class ParameterTest extends TestCase
{
    public function test_converts_to_ast(): void
    {
        $param = Parameter::positional(3);
        $node = $param->toAst();

        $paramRef = $node->getParamRef();
        static::assertNotNull($paramRef);
        static::assertSame(3, $paramRef->getNumber());
    }

    public function test_creates_aliased_expression(): void
    {
        $param = Parameter::positional(1);
        $aliased = $param->as('p');

        static::assertInstanceOf(AliasedExpression::class, $aliased);
        static::assertSame('p', $aliased->getAlias());
        static::assertSame($param, $aliased->getExpression());
    }

    public function test_creates_parameter_with_higher_number(): void
    {
        $param = Parameter::positional(42);

        static::assertSame(42, $param->number());
    }

    public function test_creates_positional_parameter(): void
    {
        $param = Parameter::positional(1);

        static::assertSame(1, $param->number());
    }

    public function test_rejects_negative_parameter(): void
    {
        $this->expectException(InvalidExpressionException::class);

        Parameter::positional(-1);
    }

    public function test_rejects_zero_parameter(): void
    {
        $this->expectException(InvalidExpressionException::class);

        Parameter::positional(0);
    }

    public function test_roundtrip_conversion(): void
    {
        $original = Parameter::positional(5);
        $node = $original->toAst();
        $reconstructed = Parameter::fromAst($node);

        static::assertEquals($original->number(), $reconstructed->number());
    }
}
