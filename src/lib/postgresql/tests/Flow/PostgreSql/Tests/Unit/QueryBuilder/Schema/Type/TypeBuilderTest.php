<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Type;

use function Flow\PostgreSql\DSL\{sql_type_text, sql_type_varchar};
use Flow\PostgreSql\Protobuf\AST\{AlterEnumStmt, CompositeTypeStmt, CreateEnumStmt, CreateRangeStmt, DropBehavior, DropStmt, ObjectType};
use Flow\PostgreSql\QueryBuilder\Schema\Type\{AlterEnumTypeBuilder, CreateCompositeTypeBuilder, CreateEnumTypeBuilder, CreateRangeTypeBuilder, DropTypeBuilder, TypeAttribute};

use PHPUnit\Framework\TestCase;

final class TypeBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_enum_type_add_value() : void
    {
        $builder = AlterEnumTypeBuilder::create('status')
            ->addValue('archived');

        $ast = $builder->toAst();

        self::assertSame('archived', $ast->getNewVal());
        self::assertSame('', $ast->getOldVal());
    }

    public function test_alter_enum_type_add_value_after() : void
    {
        $builder = AlterEnumTypeBuilder::create('status')
            ->addValueAfter('archived', 'closed');

        $ast = $builder->toAst();

        self::assertSame('archived', $ast->getNewVal());
        self::assertSame('closed', $ast->getNewValNeighbor());
        self::assertTrue($ast->getNewValIsAfter());
    }

    public function test_alter_enum_type_add_value_before() : void
    {
        $builder = AlterEnumTypeBuilder::create('status')
            ->addValueBefore('pending', 'active');

        $ast = $builder->toAst();

        self::assertSame('pending', $ast->getNewVal());
        self::assertSame('active', $ast->getNewValNeighbor());
        self::assertFalse($ast->getNewValIsAfter());
    }

    public function test_alter_enum_type_ast_type() : void
    {
        $builder = AlterEnumTypeBuilder::create('status')
            ->addValue('archived');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterEnumStmt::class, $ast);
    }

    public function test_alter_enum_type_if_not_exists() : void
    {
        $builder = AlterEnumTypeBuilder::create('status')
            ->addValue('archived')
            ->ifNotExists();

        $ast = $builder->toAst();

        self::assertTrue($ast->getSkipIfNewValExists());
    }

    public function test_alter_enum_type_rename_value() : void
    {
        $builder = AlterEnumTypeBuilder::create('status')
            ->renameValue('old_name', 'new_name');

        $ast = $builder->toAst();

        self::assertSame('old_name', $ast->getOldVal());
        self::assertSame('new_name', $ast->getNewVal());
    }

    public function test_create_composite_type_ast_type() : void
    {
        $builder = CreateCompositeTypeBuilder::create('address')
            ->attributes(TypeAttribute::of('street', sql_type_text()));

        $ast = $builder->toAst();

        self::assertInstanceOf(CompositeTypeStmt::class, $ast);
    }

    public function test_create_composite_type_sets_attributes() : void
    {
        $builder = CreateCompositeTypeBuilder::create('address')
            ->attributes(
                TypeAttribute::of('street', sql_type_text()),
                TypeAttribute::of('city', sql_type_text()),
                TypeAttribute::of('zip', sql_type_varchar(50))
            );

        $ast = $builder->toAst();
        $coldeflist = $ast->getColdeflist();

        self::assertCount(3, $coldeflist);
    }

    public function test_create_composite_type_sets_name() : void
    {
        $builder = CreateCompositeTypeBuilder::create('address')
            ->attributes(TypeAttribute::of('street', sql_type_text()));

        $ast = $builder->toAst();
        $typevar = $ast->getTypevar();

        self::assertNotNull($typevar);
        self::assertSame('address', $typevar->getRelname());
    }

    public function test_create_composite_type_with_collation() : void
    {
        $builder = CreateCompositeTypeBuilder::create('address')
            ->attributes(TypeAttribute::of('name', sql_type_text())->collate('en_US'));

        $ast = $builder->toAst();
        $coldeflist = $ast->getColdeflist();

        self::assertCount(1, $coldeflist);

        $coldef = $coldeflist[0]->getColumnDef();
        self::assertNotNull($coldef);
        self::assertNotNull($coldef->getCollClause());
    }

    public function test_create_composite_type_with_schema() : void
    {
        $builder = CreateCompositeTypeBuilder::create('public.address')
            ->attributes(TypeAttribute::of('street', sql_type_text()));

        $ast = $builder->toAst();
        $typevar = $ast->getTypevar();

        self::assertNotNull($typevar);
        self::assertSame('address', $typevar->getRelname());
        self::assertSame('public', $typevar->getSchemaname());
    }

    public function test_create_enum_type_ast_type() : void
    {
        $builder = CreateEnumTypeBuilder::create('status')
            ->labels('pending', 'active', 'closed');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateEnumStmt::class, $ast);
    }

    public function test_create_enum_type_sets_labels() : void
    {
        $builder = CreateEnumTypeBuilder::create('status')
            ->labels('pending', 'active', 'closed');

        $ast = $builder->toAst();
        $vals = $ast->getVals();

        self::assertCount(3, $vals);
    }

    public function test_create_enum_type_sets_name() : void
    {
        $builder = CreateEnumTypeBuilder::create('status')
            ->labels('pending', 'active');

        $ast = $builder->toAst();
        $typeName = $ast->getTypeName();

        self::assertCount(1, $typeName);
    }

    public function test_create_enum_type_with_schema() : void
    {
        $builder = CreateEnumTypeBuilder::create('public.status')
            ->labels('pending', 'active');

        $ast = $builder->toAst();
        $typeName = $ast->getTypeName();

        self::assertCount(2, $typeName);
    }

    public function test_create_range_type_ast_type() : void
    {
        $builder = CreateRangeTypeBuilder::create('floatrange')
            ->subtype('float8');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateRangeStmt::class, $ast);
    }

    public function test_create_range_type_sets_name() : void
    {
        $builder = CreateRangeTypeBuilder::create('floatrange')
            ->subtype('float8');

        $ast = $builder->toAst();
        $typeName = $ast->getTypeName();

        self::assertCount(1, $typeName);
    }

    public function test_create_range_type_sets_subtype() : void
    {
        $builder = CreateRangeTypeBuilder::create('floatrange')
            ->subtype('float8');

        $ast = $builder->toAst();
        $params = $ast->getParams();

        self::assertCount(1, $params);

        $defElem = $params[0]->getDefElem();
        self::assertNotNull($defElem);
        self::assertSame('subtype', $defElem->getDefname());
    }

    public function test_create_range_type_with_options() : void
    {
        $builder = CreateRangeTypeBuilder::create('floatrange')
            ->subtype('float8')
            ->subtypeOpclass('float8_ops')
            ->collation('en_US');

        $ast = $builder->toAst();
        $params = $ast->getParams();

        self::assertCount(3, $params);
    }

    public function test_create_range_type_with_schema() : void
    {
        $builder = CreateRangeTypeBuilder::create('public.floatrange')
            ->subtype('float8');

        $ast = $builder->toAst();
        $typeName = $ast->getTypeName();

        self::assertCount(2, $typeName);
    }

    public function test_drop_type_ast_type() : void
    {
        $builder = DropTypeBuilder::create('address');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_TYPE, $ast->getRemoveType());
    }

    public function test_drop_type_cascade() : void
    {
        $builder = DropTypeBuilder::create('address')
            ->cascade();

        $ast = $builder->toAst();

        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_type_if_exists() : void
    {
        $builder = DropTypeBuilder::create('address')
            ->ifExists();

        $ast = $builder->toAst();

        self::assertTrue($ast->getMissingOk());
    }

    public function test_drop_type_immutability() : void
    {
        $original = DropTypeBuilder::create('address');
        $modified = $original->ifExists();

        self::assertFalse($original->toAst()->getMissingOk());
        self::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_drop_type_multiple_types() : void
    {
        $builder = DropTypeBuilder::create('address', 'status', 'priority');

        $ast = $builder->toAst();

        self::assertCount(3, $ast->getObjects());
    }

    public function test_drop_type_restrict() : void
    {
        $builder = DropTypeBuilder::create('address')
            ->restrict();

        $ast = $builder->toAst();

        self::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }
}
