<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{Node, PBString, TypeCast as AstTypeCast, TypeName};
use Flow\PostgreSql\QueryBuilder\Expression\{Column, Literal, TypeCast};
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use PHPUnit\Framework\TestCase;

final class TypeCastTest extends TestCase
{
    public function test_complex_expression_cast() : void
    {
        $innerExpr = new TypeCast(Literal::string('42'), ColumnType::integer());
        $outerCast = new TypeCast($innerExpr, ColumnType::varchar(255));

        $node = $outerCast->toAst();
        $restored = TypeCast::fromAst($node);

        self::assertInstanceOf(TypeCast::class, $restored->getExpression());
    }

    public function test_converts_to_ast() : void
    {
        $expr = Literal::string('123');
        $cast = new TypeCast($expr, ColumnType::integer());

        $node = $cast->toAst();

        $typeCast = $node->getTypeCast();
        self::assertNotNull($typeCast);
        self::assertNotNull($typeCast->getArg());

        $typeName = $typeCast->getTypeName();
        self::assertNotNull($typeName);

        $namesNodes = $typeName->getNames();
        self::assertCount(2, $namesNodes);
        $schemaString = $namesNodes[0]->getString();
        self::assertNotNull($schemaString);
        self::assertSame('pg_catalog', $schemaString->getSval());
        $typeString = $namesNodes[1]->getString();
        self::assertNotNull($typeString);
        self::assertSame('int4', $typeString->getSval());
    }

    public function test_converts_type_cast_with_schema_to_ast() : void
    {
        $expr = Column::name('id');
        $cast = new TypeCast($expr, ColumnType::text());

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
        $cast = new TypeCast($expr, ColumnType::varchar(100));
        $aliased = $cast->as('casted_value');

        self::assertSame('casted_value', $aliased->getAlias());
        self::assertSame($cast, $aliased->getExpression());
    }

    public function test_creates_simple_type_cast() : void
    {
        $expr = Literal::int(42);
        $dataType = ColumnType::varchar(100);
        $cast = new TypeCast($expr, $dataType);

        self::assertSame($dataType, $cast->getColumnType());
        self::assertSame($expr, $cast->getExpression());
    }

    public function test_creates_type_cast_with_schema() : void
    {
        $expr = Column::name('value');
        $dataType = ColumnType::integer();
        $cast = new TypeCast($expr, $dataType);

        self::assertSame($dataType, $cast->getColumnType());
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

        self::assertInstanceOf(ColumnType::class, $cast->getColumnType());
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

        self::assertInstanceOf(ColumnType::class, $cast->getColumnType());
    }

    public function test_round_trip_conversion() : void
    {
        $expr = Column::name('amount');
        $cast = new TypeCast($expr, ColumnType::numeric(10, 2));

        $node = $cast->toAst();
        $restored = TypeCast::fromAst($node);

        self::assertInstanceOf(ColumnType::class, $restored->getColumnType());
    }

    public function test_with_column_type_creates_new_instance() : void
    {
        $expr = Literal::int(1);
        $dataType1 = ColumnType::integer();
        $dataType2 = ColumnType::varchar(100);
        $cast = new TypeCast($expr, $dataType1);

        $newCast = $cast->withColumnType($dataType2);

        self::assertNotSame($cast, $newCast);
        self::assertSame($dataType1, $cast->getColumnType());
        self::assertSame($dataType2, $newCast->getColumnType());
    }

    public function test_with_expression_creates_new_instance() : void
    {
        $expr1 = Literal::int(1);
        $expr2 = Literal::int(2);
        $cast = new TypeCast($expr1, ColumnType::integer());

        $newCast = $cast->withExpression($expr2);

        self::assertNotSame($cast, $newCast);
        self::assertSame($expr1, $cast->getExpression());
        self::assertSame($expr2, $newCast->getExpression());
    }
}
