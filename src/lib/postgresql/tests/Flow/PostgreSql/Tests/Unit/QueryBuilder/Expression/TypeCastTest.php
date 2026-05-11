<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\TypeCast as AstTypeCast;
use Flow\PostgreSql\Protobuf\AST\TypeName;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use Flow\PostgreSql\QueryBuilder\Expression\TypeCast;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use PHPUnit\Framework\TestCase;

final class TypeCastTest extends TestCase
{
    public function test_complex_expression_cast(): void
    {
        $innerExpr = new TypeCast(Literal::string('42'), ColumnType::integer());
        $outerCast = new TypeCast($innerExpr, ColumnType::varchar(255));

        $node = $outerCast->toAst();
        $restored = TypeCast::fromAst($node);

        static::assertInstanceOf(TypeCast::class, $restored->getExpression());
    }

    public function test_converts_to_ast(): void
    {
        $expr = Literal::string('123');
        $cast = new TypeCast($expr, ColumnType::integer());

        $node = $cast->toAst();

        $typeCast = $node->getTypeCast();
        static::assertNotNull($typeCast);
        static::assertNotNull($typeCast->getArg());

        $typeName = $typeCast->getTypeName();
        static::assertNotNull($typeName);

        $namesNodes = $typeName->getNames();
        static::assertCount(2, $namesNodes);
        $schemaString = $namesNodes[0]->getString();
        static::assertNotNull($schemaString);
        static::assertSame('pg_catalog', $schemaString->getSval());
        $typeString = $namesNodes[1]->getString();
        static::assertNotNull($typeString);
        static::assertSame('int4', $typeString->getSval());
    }

    public function test_converts_type_cast_with_schema_to_ast(): void
    {
        $expr = Column::name('id');
        $cast = new TypeCast($expr, ColumnType::text());

        $node = $cast->toAst();
        $typeCast = $node->getTypeCast();
        static::assertNotNull($typeCast);
        $typeName = $typeCast->getTypeName();
        static::assertNotNull($typeName);
        $namesNodes = $typeName->getNames();

        static::assertCount(2, $namesNodes);
        $firstString = $namesNodes[0]->getString();
        static::assertNotNull($firstString);
        static::assertSame('pg_catalog', $firstString->getSval());
        $secondString = $namesNodes[1]->getString();
        static::assertNotNull($secondString);
        static::assertSame('text', $secondString->getSval());
    }

    public function test_creates_aliased_expression(): void
    {
        $expr = Literal::int(42);
        $cast = new TypeCast($expr, ColumnType::varchar(100));
        $aliased = $cast->as('casted_value');

        static::assertSame('casted_value', $aliased->getAlias());
        static::assertSame($cast, $aliased->getExpression());
    }

    public function test_creates_simple_type_cast(): void
    {
        $expr = Literal::int(42);
        $dataType = ColumnType::varchar(100);
        $cast = new TypeCast($expr, $dataType);

        static::assertSame($dataType, $cast->getColumnType());
        static::assertSame($expr, $cast->getExpression());
    }

    public function test_creates_type_cast_with_schema(): void
    {
        $expr = Column::name('value');
        $dataType = ColumnType::integer();
        $cast = new TypeCast($expr, $dataType);

        static::assertSame($dataType, $cast->getColumnType());
    }

    public function test_recreates_from_ast(): void
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

        static::assertInstanceOf(ColumnType::class, $cast->getColumnType());
    }

    public function test_recreates_type_cast_with_schema_from_ast(): void
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

        static::assertInstanceOf(ColumnType::class, $cast->getColumnType());
    }

    public function test_round_trip_conversion(): void
    {
        $expr = Column::name('amount');
        $cast = new TypeCast($expr, ColumnType::numeric(10, 2));

        $node = $cast->toAst();
        $restored = TypeCast::fromAst($node);

        static::assertInstanceOf(ColumnType::class, $restored->getColumnType());
    }

    public function test_with_column_type_creates_new_instance(): void
    {
        $expr = Literal::int(1);
        $dataType1 = ColumnType::integer();
        $dataType2 = ColumnType::varchar(100);
        $cast = new TypeCast($expr, $dataType1);

        $newCast = $cast->withColumnType($dataType2);

        static::assertNotSame($cast, $newCast);
        static::assertSame($dataType1, $cast->getColumnType());
        static::assertSame($dataType2, $newCast->getColumnType());
    }

    public function test_with_expression_creates_new_instance(): void
    {
        $expr1 = Literal::int(1);
        $expr2 = Literal::int(2);
        $cast = new TypeCast($expr1, ColumnType::integer());

        $newCast = $cast->withExpression($expr2);

        static::assertNotSame($cast, $newCast);
        static::assertSame($expr1, $cast->getExpression());
        static::assertSame($expr2, $newCast->getExpression());
    }
}
