<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{Boolean, Integer};
use Flow\PgQuery\QueryBuilder\Expression\{AliasedExpression, Literal};
use PHPUnit\Framework\TestCase;

final class LiteralTest extends TestCase
{
    public function test_converts_bool_to_ast() : void
    {
        $literal = Literal::bool(true);
        $node = $literal->toAst();

        $aConst = $node->getAConst();
        self::assertNotNull($aConst);

        $booleanObj = $aConst->getBoolval();
        self::assertInstanceOf(Boolean::class, $booleanObj);
        self::assertTrue($booleanObj->getBoolval());
    }

    public function test_converts_float_to_ast() : void
    {
        $literal = Literal::float(3.14);
        $node = $literal->toAst();

        $aConst = $node->getAConst();
        self::assertNotNull($aConst);

        $fval = $aConst->getFval();
        self::assertNotNull($fval);
        self::assertSame('3.14', $fval->getFval());
    }

    public function test_converts_int_to_ast() : void
    {
        $literal = Literal::int(123);
        $node = $literal->toAst();

        $aConst = $node->getAConst();
        self::assertNotNull($aConst);
        self::assertFalse($aConst->getIsnull());

        $integerObj = $aConst->getIval();
        self::assertInstanceOf(Integer::class, $integerObj);
        self::assertSame(123, $integerObj->getIval());
    }

    public function test_converts_null_to_ast() : void
    {
        $literal = Literal::null();
        $node = $literal->toAst();

        $aConst = $node->getAConst();
        self::assertNotNull($aConst);
        self::assertTrue($aConst->getIsnull());
    }

    public function test_converts_string_to_ast() : void
    {
        $literal = Literal::string('test');
        $node = $literal->toAst();

        $aConst = $node->getAConst();
        self::assertNotNull($aConst);

        $sval = $aConst->getSval();
        self::assertNotNull($sval);
        self::assertSame('test', $sval->getSval());
    }

    public function test_creates_aliased_expression() : void
    {
        $literal = Literal::int(42);
        $aliased = $literal->as('answer');

        self::assertInstanceOf(AliasedExpression::class, $aliased);
        self::assertSame('answer', $aliased->getAlias());
        self::assertSame($literal, $aliased->getExpression());
    }

    public function test_creates_bool_literal() : void
    {
        $literal = Literal::bool(true);

        self::assertTrue($literal->value());
        self::assertTrue($literal->isBool());
        self::assertFalse($literal->isString());
        self::assertFalse($literal->isInt());
        self::assertFalse($literal->isFloat());
        self::assertFalse($literal->isNull());
    }

    public function test_creates_float_literal() : void
    {
        $literal = Literal::float(3.14);

        self::assertSame(3.14, $literal->value());
        self::assertTrue($literal->isFloat());
        self::assertFalse($literal->isString());
        self::assertFalse($literal->isInt());
        self::assertFalse($literal->isBool());
        self::assertFalse($literal->isNull());
    }

    public function test_creates_int_literal() : void
    {
        $literal = Literal::int(42);

        self::assertSame(42, $literal->value());
        self::assertTrue($literal->isInt());
        self::assertFalse($literal->isString());
        self::assertFalse($literal->isFloat());
        self::assertFalse($literal->isBool());
        self::assertFalse($literal->isNull());
    }

    public function test_creates_null_literal() : void
    {
        $literal = Literal::null();

        self::assertNull($literal->value());
        self::assertTrue($literal->isNull());
        self::assertFalse($literal->isString());
        self::assertFalse($literal->isInt());
        self::assertFalse($literal->isFloat());
        self::assertFalse($literal->isBool());
    }

    public function test_creates_string_literal() : void
    {
        $literal = Literal::string('hello');

        self::assertSame('hello', $literal->value());
        self::assertTrue($literal->isString());
        self::assertFalse($literal->isInt());
        self::assertFalse($literal->isFloat());
        self::assertFalse($literal->isBool());
        self::assertFalse($literal->isNull());
    }

    public function test_roundtrip_bool_conversion() : void
    {
        $original = Literal::bool(false);
        $node = $original->toAst();
        $reconstructed = Literal::fromAst($node);

        self::assertEquals($original->value(), $reconstructed->value());
    }

    public function test_roundtrip_float_conversion() : void
    {
        $original = Literal::float(2.718);
        $node = $original->toAst();
        $reconstructed = Literal::fromAst($node);

        self::assertEquals($original->value(), $reconstructed->value());
    }

    public function test_roundtrip_int_conversion() : void
    {
        $original = Literal::int(999);
        $node = $original->toAst();
        $reconstructed = Literal::fromAst($node);

        self::assertEquals($original->value(), $reconstructed->value());
    }

    public function test_roundtrip_null_conversion() : void
    {
        $original = Literal::null();
        $node = $original->toAst();
        $reconstructed = Literal::fromAst($node);

        self::assertTrue($reconstructed->isNull());
    }

    public function test_roundtrip_string_conversion() : void
    {
        $original = Literal::string('hello world');
        $node = $original->toAst();
        $reconstructed = Literal::fromAst($node);

        self::assertEquals($original->value(), $reconstructed->value());
    }
}
