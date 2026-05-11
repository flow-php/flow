<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Type;

use Flow\PostgreSql\Protobuf\AST\AlterEnumStmt;
use Flow\PostgreSql\Protobuf\AST\CompositeTypeStmt;
use Flow\PostgreSql\Protobuf\AST\CreateEnumStmt;
use Flow\PostgreSql\Protobuf\AST\CreateRangeStmt;
use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\QueryBuilder\Schema\Type\AlterEnumTypeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Type\CreateCompositeTypeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Type\CreateEnumTypeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Type\CreateRangeTypeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Type\DropTypeBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Type\TypeAttribute;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\type_attr;

final class TypeBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_enum_type_add_value(): void
    {
        $builder = AlterEnumTypeBuilder::create('status')->addValue('archived');

        $ast = $builder->toAst();

        static::assertSame('archived', $ast->getNewVal());
        static::assertSame('', $ast->getOldVal());
    }

    public function test_alter_enum_type_add_value_after(): void
    {
        $builder = AlterEnumTypeBuilder::create('status')->addValueAfter('archived', 'closed');

        $ast = $builder->toAst();

        static::assertSame('archived', $ast->getNewVal());
        static::assertSame('closed', $ast->getNewValNeighbor());
        static::assertTrue($ast->getNewValIsAfter());
    }

    public function test_alter_enum_type_add_value_before(): void
    {
        $builder = AlterEnumTypeBuilder::create('status')->addValueBefore('pending', 'active');

        $ast = $builder->toAst();

        static::assertSame('pending', $ast->getNewVal());
        static::assertSame('active', $ast->getNewValNeighbor());
        static::assertFalse($ast->getNewValIsAfter());
    }

    public function test_alter_enum_type_add_value_if_not_exists_to_sql(): void
    {
        static::assertSame(
            "ALTER TYPE status ADD VALUE IF NOT EXISTS 'archived'",
            alter()->enumType('status')->addValue('archived')->ifNotExists()->toSql(),
        );
    }

    public function test_alter_enum_type_ast_type(): void
    {
        $builder = AlterEnumTypeBuilder::create('status')->addValue('archived');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterEnumStmt::class, $ast);
    }

    public function test_alter_enum_type_if_not_exists(): void
    {
        $builder = AlterEnumTypeBuilder::create('status')->addValue('archived')->ifNotExists();

        $ast = $builder->toAst();

        static::assertTrue($ast->getSkipIfNewValExists());
    }

    public function test_alter_enum_type_rename_value(): void
    {
        $builder = AlterEnumTypeBuilder::create('status')->renameValue('old_name', 'new_name');

        $ast = $builder->toAst();

        static::assertSame('old_name', $ast->getOldVal());
        static::assertSame('new_name', $ast->getNewVal());
    }

    public function test_create_composite_type_ast_type(): void
    {
        $builder = CreateCompositeTypeBuilder::create('address')->attributes(TypeAttribute::of(
            'street',
            column_type_text(),
        ));

        $ast = $builder->toAst();

        static::assertInstanceOf(CompositeTypeStmt::class, $ast);
    }

    public function test_create_composite_type_sets_attributes(): void
    {
        $builder = CreateCompositeTypeBuilder::create('address')->attributes(
            TypeAttribute::of('street', column_type_text()),
            TypeAttribute::of('city', column_type_text()),
            TypeAttribute::of('zip', column_type_varchar(50)),
        );

        $ast = $builder->toAst();
        $coldeflist = $ast->getColdeflist();

        static::assertCount(3, $coldeflist);
    }

    public function test_create_composite_type_sets_name(): void
    {
        $builder = CreateCompositeTypeBuilder::create('address')->attributes(TypeAttribute::of(
            'street',
            column_type_text(),
        ));

        $ast = $builder->toAst();
        $typevar = $ast->getTypevar();

        static::assertNotNull($typevar);
        static::assertSame('address', $typevar->getRelname());
    }

    public function test_create_composite_type_simple_to_sql(): void
    {
        static::assertSame(
            'CREATE TYPE address AS (street pg_catalog.text, city pg_catalog.text, zip pg_catalog.text)',
            create()
                ->compositeType('address')
                ->attributes(
                    type_attr('street', column_type_text()),
                    type_attr('city', column_type_text()),
                    type_attr('zip', column_type_text()),
                )
                ->toSql(),
        );
    }

    public function test_create_composite_type_with_collation(): void
    {
        $builder = CreateCompositeTypeBuilder::create('address')->attributes(TypeAttribute::of(
            'name',
            column_type_text(),
        )->collate('en_US'));

        $ast = $builder->toAst();
        $coldeflist = $ast->getColdeflist();

        static::assertCount(1, $coldeflist);

        $coldef = $coldeflist[0]->getColumnDef();
        static::assertNotNull($coldef);
        static::assertNotNull($coldef->getCollClause());
    }

    public function test_create_composite_type_with_schema(): void
    {
        $builder = CreateCompositeTypeBuilder::create('public.address')->attributes(TypeAttribute::of(
            'street',
            column_type_text(),
        ));

        $ast = $builder->toAst();
        $typevar = $ast->getTypevar();

        static::assertNotNull($typevar);
        static::assertSame('address', $typevar->getRelname());
        static::assertSame('public', $typevar->getSchemaname());
    }

    public function test_create_enum_type_ast_type(): void
    {
        $builder = CreateEnumTypeBuilder::create('status')->labels('pending', 'active', 'closed');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateEnumStmt::class, $ast);
    }

    public function test_create_enum_type_sets_labels(): void
    {
        $builder = CreateEnumTypeBuilder::create('status')->labels('pending', 'active', 'closed');

        $ast = $builder->toAst();
        $vals = $ast->getVals();

        static::assertCount(3, $vals);
    }

    public function test_create_enum_type_sets_name(): void
    {
        $builder = CreateEnumTypeBuilder::create('status')->labels('pending', 'active');

        $ast = $builder->toAst();
        $typeName = $ast->getTypeName();

        static::assertCount(1, $typeName);
    }

    public function test_create_enum_type_simple_to_sql(): void
    {
        static::assertSame(
            "CREATE TYPE status AS ENUM ('pending', 'active', 'closed')",
            create()->enumType('status')->labels('pending', 'active', 'closed')->toSql(),
        );
    }

    public function test_create_enum_type_with_schema(): void
    {
        $builder = CreateEnumTypeBuilder::create('public.status')->labels('pending', 'active');

        $ast = $builder->toAst();
        $typeName = $ast->getTypeName();

        static::assertCount(2, $typeName);
    }

    public function test_create_range_type_ast_type(): void
    {
        $builder = CreateRangeTypeBuilder::create('floatrange')->subtype('float8');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateRangeStmt::class, $ast);
    }

    public function test_create_range_type_sets_name(): void
    {
        $builder = CreateRangeTypeBuilder::create('floatrange')->subtype('float8');

        $ast = $builder->toAst();
        $typeName = $ast->getTypeName();

        static::assertCount(1, $typeName);
    }

    public function test_create_range_type_sets_subtype(): void
    {
        $builder = CreateRangeTypeBuilder::create('floatrange')->subtype('float8');

        $ast = $builder->toAst();
        $params = $ast->getParams();

        static::assertCount(1, $params);

        $defElem = $params[0]->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('subtype', $defElem->getDefname());
    }

    public function test_create_range_type_simple_to_sql(): void
    {
        static::assertSame(
            'CREATE TYPE floatrange AS RANGE (subtype = float8)',
            create()->rangeType('floatrange')->subtype('float8')->toSql(),
        );
    }

    public function test_create_range_type_with_collation_to_sql(): void
    {
        static::assertSame(
            "CREATE TYPE textrange AS RANGE (subtype = text, \"collation\" = 'en_US')",
            create()->rangeType('textrange')->subtype('text')->collation('en_US')->toSql(),
        );
    }

    public function test_create_range_type_with_options(): void
    {
        $builder = CreateRangeTypeBuilder::create('floatrange')
            ->subtype('float8')
            ->subtypeOpclass('float8_ops')
            ->collation('en_US');

        $ast = $builder->toAst();
        $params = $ast->getParams();

        static::assertCount(3, $params);
    }

    public function test_create_range_type_with_schema(): void
    {
        $builder = CreateRangeTypeBuilder::create('public.floatrange')->subtype('float8');

        $ast = $builder->toAst();
        $typeName = $ast->getTypeName();

        static::assertCount(2, $typeName);
    }

    public function test_drop_type_ast_type(): void
    {
        $builder = DropTypeBuilder::create('address');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_TYPE, $ast->getRemoveType());
    }

    public function test_drop_type_cascade(): void
    {
        $builder = DropTypeBuilder::create('address')->cascade();

        $ast = $builder->toAst();

        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_type_if_exists(): void
    {
        $builder = DropTypeBuilder::create('address')->ifExists();

        $ast = $builder->toAst();

        static::assertTrue($ast->getMissingOk());
    }

    public function test_drop_type_if_exists_cascade_to_sql(): void
    {
        static::assertSame(
            'DROP TYPE IF EXISTS address CASCADE',
            drop()->type('address')->ifExists()->cascade()->toSql(),
        );
    }

    public function test_drop_type_immutability(): void
    {
        $original = DropTypeBuilder::create('address');
        $modified = $original->ifExists();

        static::assertFalse($original->toAst()->getMissingOk());
        static::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_drop_type_multiple_to_sql(): void
    {
        static::assertSame(
            'DROP TYPE address, status, floatrange',
            drop()->type('address', 'status', 'floatrange')->toSql(),
        );
    }

    public function test_drop_type_multiple_types(): void
    {
        $builder = DropTypeBuilder::create('address', 'status', 'priority');

        $ast = $builder->toAst();

        static::assertCount(3, $ast->getObjects());
    }

    public function test_drop_type_restrict(): void
    {
        $builder = DropTypeBuilder::create('address')->restrict();

        $ast = $builder->toAst();

        static::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_drop_type_simple_to_sql(): void
    {
        static::assertSame('DROP TYPE address', drop()->type('address')->toSql());
    }
}
