<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\Boolean;
use Flow\PostgreSql\Protobuf\AST\Integer;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;

final class LiteralTest extends TestCase
{
    public function test_converts_bool_to_ast(): void
    {
        $literal = Literal::bool(true);
        $node = $literal->toAst();

        $aConst = $node->getAConst();
        static::assertNotNull($aConst);

        $booleanObj = type_instance_of(Boolean::class)->assert($aConst->getBoolval());
        static::assertTrue($booleanObj->getBoolval());
    }

    public function test_converts_float_to_ast(): void
    {
        $literal = Literal::float(3.14);
        $node = $literal->toAst();

        $aConst = $node->getAConst();
        static::assertNotNull($aConst);

        $fval = $aConst->getFval();
        static::assertNotNull($fval);
        static::assertSame('3.14', $fval->getFval());
    }

    public function test_converts_int_to_ast(): void
    {
        $literal = Literal::int(123);
        $node = $literal->toAst();

        $aConst = $node->getAConst();
        static::assertNotNull($aConst);
        static::assertFalse($aConst->getIsnull());

        $integerObj = type_instance_of(Integer::class)->assert($aConst->getIval());
        static::assertSame(123, $integerObj->getIval());
    }

    public function test_converts_null_to_ast(): void
    {
        $literal = Literal::null();
        $node = $literal->toAst();

        $aConst = $node->getAConst();
        static::assertNotNull($aConst);
        static::assertTrue($aConst->getIsnull());
    }

    public function test_converts_string_to_ast(): void
    {
        $literal = Literal::string('test');
        $node = $literal->toAst();

        $aConst = $node->getAConst();
        static::assertNotNull($aConst);

        $sval = $aConst->getSval();
        static::assertNotNull($sval);
        static::assertSame('test', $sval->getSval());
    }

    public function test_creates_aliased_expression(): void
    {
        $literal = Literal::int(42);
        $aliased = $literal->as('answer');

        static::assertInstanceOf(AliasedExpression::class, $aliased);
        static::assertSame('answer', $aliased->getAlias());
        static::assertSame($literal, $aliased->getExpression());
    }

    public function test_creates_bool_literal(): void
    {
        $literal = Literal::bool(true);

        static::assertTrue($literal->value());
        static::assertTrue($literal->isBool());
        static::assertFalse($literal->isString());
        static::assertFalse($literal->isInt());
        static::assertFalse($literal->isFloat());
        static::assertFalse($literal->isNull());
    }

    public function test_creates_float_literal(): void
    {
        $literal = Literal::float(3.14);

        static::assertSame(3.14, $literal->value());
        static::assertTrue($literal->isFloat());
        static::assertFalse($literal->isString());
        static::assertFalse($literal->isInt());
        static::assertFalse($literal->isBool());
        static::assertFalse($literal->isNull());
    }

    public function test_creates_int_literal(): void
    {
        $literal = Literal::int(42);

        static::assertSame(42, $literal->value());
        static::assertTrue($literal->isInt());
        static::assertFalse($literal->isString());
        static::assertFalse($literal->isFloat());
        static::assertFalse($literal->isBool());
        static::assertFalse($literal->isNull());
    }

    public function test_creates_null_literal(): void
    {
        $literal = Literal::null();

        static::assertNull($literal->value());
        static::assertTrue($literal->isNull());
        static::assertFalse($literal->isString());
        static::assertFalse($literal->isInt());
        static::assertFalse($literal->isFloat());
        static::assertFalse($literal->isBool());
    }

    public function test_creates_string_literal(): void
    {
        $literal = Literal::string('hello');

        static::assertSame('hello', $literal->value());
        static::assertTrue($literal->isString());
        static::assertFalse($literal->isInt());
        static::assertFalse($literal->isFloat());
        static::assertFalse($literal->isBool());
        static::assertFalse($literal->isNull());
    }

    public function test_roundtrip_bool_conversion(): void
    {
        $original = Literal::bool(false);
        $node = $original->toAst();
        $reconstructed = Literal::fromAst($node);

        static::assertEquals($original->value(), $reconstructed->value());
    }

    public function test_roundtrip_float_conversion(): void
    {
        $original = Literal::float(2.718);
        $node = $original->toAst();
        $reconstructed = Literal::fromAst($node);

        static::assertEquals($original->value(), $reconstructed->value());
    }

    public function test_roundtrip_int_conversion(): void
    {
        $original = Literal::int(999);
        $node = $original->toAst();
        $reconstructed = Literal::fromAst($node);

        static::assertEquals($original->value(), $reconstructed->value());
    }

    public function test_roundtrip_null_conversion(): void
    {
        $original = Literal::null();
        $node = $original->toAst();
        $reconstructed = Literal::fromAst($node);

        static::assertTrue($reconstructed->isNull());
    }

    public function test_roundtrip_string_conversion(): void
    {
        $original = Literal::string('hello world');
        $node = $original->toAst();
        $reconstructed = Literal::fromAst($node);

        static::assertEquals($original->value(), $reconstructed->value());
    }
}
