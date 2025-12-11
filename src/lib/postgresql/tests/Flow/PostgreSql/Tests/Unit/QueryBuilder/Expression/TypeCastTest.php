<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{Node, PBString, TypeCast as AstTypeCast, TypeName};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidExpressionException;
use Flow\PostgreSql\QueryBuilder\Expression\{Column, Literal, TypeCast};
use PHPUnit\Framework\TestCase;

final class TypeCastTest extends TestCase
{
    public function test_complex_expression_cast() : void
    {
        $innerExpr = new TypeCast(Literal::string('42'), ['integer']);
        $outerCast = new TypeCast($innerExpr, ['varchar']);

        $node = $outerCast->toAst();
        $restored = TypeCast::fromAst($node);

        self::assertSame(['varchar'], $restored->getTypeName());
        self::assertInstanceOf(TypeCast::class, $restored->getExpression());
    }

    public function test_converts_to_ast() : void
    {
        $expr = Literal::string('123');
        $cast = new TypeCast($expr, ['integer']);

        $node = $cast->toAst();

        $typeCast = $node->getTypeCast();
        self::assertNotNull($typeCast);
        self::assertNotNull($typeCast->getArg());

        $typeName = $typeCast->getTypeName();
        self::assertNotNull($typeName);

        $namesNodes = $typeName->getNames();
        self::assertCount(1, $namesNodes);
        $stringNode = $namesNodes[0]->getString();
        self::assertNotNull($stringNode);
        self::assertSame('integer', $stringNode->getSval());
    }

    public function test_converts_type_cast_with_schema_to_ast() : void
    {
        $expr = Column::name('id');
        $cast = new TypeCast($expr, ['pg_catalog', 'text']);

        $node = $cast->toAst();
        $typeCast = $node->getTypeCast();
        self::assertNotNull($typeCast);
        $typeName = $typeCast->getTypeName();
        self::assertNotNull($typeName);
        $namesNodes = $typeName->getNames();

        self::assertCount(2, $namesNodes);
        $firstString = $namesNodes[0]->getString();
        self::assertNotNull($firstString);
        self::assertSame('pg_catalog', $firstString->getSval());
        $secondString = $namesNodes[1]->getString();
        self::assertNotNull($secondString);
        self::assertSame('text', $secondString->getSval());
    }

    public function test_creates_aliased_expression() : void
    {
        $expr = Literal::int(42);
        $cast = new TypeCast($expr, ['varchar']);
        $aliased = $cast->as('casted_value');

        self::assertSame('casted_value', $aliased->getAlias());
        self::assertSame($cast, $aliased->getExpression());
    }

    public function test_creates_simple_type_cast() : void
    {
        $expr = Literal::int(42);
        $cast = new TypeCast($expr, ['varchar']);

        self::assertSame(['varchar'], $cast->getTypeName());
        self::assertSame($expr, $cast->getExpression());
    }

    public function test_creates_type_cast_with_schema() : void
    {
        $expr = Column::name('value');
        $cast = new TypeCast($expr, ['pg_catalog', 'int4']);

        self::assertSame(['pg_catalog', 'int4'], $cast->getTypeName());
    }

    public function test_recreates_from_ast() : void
    {
        $stringNode = new PBString();
        $stringNode->setSval('varchar');

        $nameNode = new Node();
        $nameNode->setString($stringNode);

        $typeName = new TypeName();
        $typeName->setNames([$nameNode]);

        $argNode = Literal::int(42)->toAst();

        $typeCast = new AstTypeCast();
        $typeCast->setArg($argNode);
        $typeCast->setTypeName($typeName);

        $node = new Node();
        $node->setTypeCast($typeCast);

        $cast = TypeCast::fromAst($node);

        self::assertSame(['varchar'], $cast->getTypeName());
    }

    public function test_recreates_type_cast_with_schema_from_ast() : void
    {
        $schemaNode = new PBString();
        $schemaNode->setSval('pg_catalog');
        $schemaNameNode = new Node();
        $schemaNameNode->setString($schemaNode);

        $typeNode = new PBString();
        $typeNode->setSval('int4');
        $typeNameNode = new Node();
        $typeNameNode->setString($typeNode);

        $typeName = new TypeName();
        $typeName->setNames([$schemaNameNode, $typeNameNode]);

        $argNode = Column::name('value')->toAst();

        $typeCast = new AstTypeCast();
        $typeCast->setArg($argNode);
        $typeCast->setTypeName($typeName);

        $node = new Node();
        $node->setTypeCast($typeCast);

        $cast = TypeCast::fromAst($node);

        self::assertSame(['pg_catalog', 'int4'], $cast->getTypeName());
    }

    public function test_round_trip_conversion() : void
    {
        $expr = Column::name('amount');
        $cast = new TypeCast($expr, ['pg_catalog', 'numeric']);

        $node = $cast->toAst();
        $restored = TypeCast::fromAst($node);

        self::assertSame($cast->getTypeName(), $restored->getTypeName());
    }

    public function test_throws_exception_for_empty_type_name() : void
    {
        $this->expectException(InvalidExpressionException::class);

        $expr = Literal::int(1);
        /** @phpstan-ignore argument.type (intentionally testing exception) */
        new TypeCast($expr, []);
    }

    public function test_with_expression_creates_new_instance() : void
    {
        $expr1 = Literal::int(1);
        $expr2 = Literal::int(2);
        $cast = new TypeCast($expr1, ['integer']);

        $newCast = $cast->withExpression($expr2);

        self::assertNotSame($cast, $newCast);
        self::assertSame($expr1, $cast->getExpression());
        self::assertSame($expr2, $newCast->getExpression());
    }

    public function test_with_type_name_creates_new_instance() : void
    {
        $expr = Literal::int(1);
        $cast = new TypeCast($expr, ['integer']);

        $newCast = $cast->withTypeName('varchar');

        self::assertNotSame($cast, $newCast);
        self::assertSame(['integer'], $cast->getTypeName());
        self::assertSame(['varchar'], $newCast->getTypeName());
    }

    public function test_with_type_name_throws_exception_for_empty_array() : void
    {
        $this->expectException(InvalidExpressionException::class);

        $expr = Literal::int(1);
        $cast = new TypeCast($expr, ['integer']);

        $cast->withTypeName();
    }
}
