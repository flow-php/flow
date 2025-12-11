<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\View;

use function Flow\PostgreSql\DSL\{star, table};
use Flow\PostgreSql\Protobuf\AST\{AlterObjectSchemaStmt, AlterTableStmt, CreateTableAsStmt, DropStmt, ObjectType, RefreshMatViewStmt, RenameStmt, ViewStmt};
use Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView\AlterMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\AlterView\AlterViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\CreateMaterializedView\CreateMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\CreateView\CreateViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\DropMaterializedView\DropMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\DropView\DropViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\RefreshMaterializedView\RefreshMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use PHPUnit\Framework\TestCase;

final class ViewBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_materialized_view_set_tablespace_ast_type() : void
    {
        $builder = AlterMaterializedViewBuilder::create('my_matview')
            ->setTablespace('fast_storage');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterTableStmt::class, $ast);
    }

    public function test_alter_materialized_view_set_tablespace_if_exists_sets_flag() : void
    {
        $builder = AlterMaterializedViewBuilder::create('my_matview')
            ->ifExists()
            ->setTablespace('fast_storage');

        $ast = $builder->toAst();

        self::assertTrue($ast->getMissingOk());
    }

    public function test_alter_view_owner_to_ast_type() : void
    {
        $builder = AlterViewBuilder::create('my_view')
            ->ownerTo('new_owner');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterTableStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_VIEW, $ast->getObjtype());
    }

    public function test_alter_view_rename_ast_type() : void
    {
        $builder = AlterViewBuilder::create('old_view')
            ->renameTo('new_view');

        $ast = $builder->toAst();

        self::assertInstanceOf(RenameStmt::class, $ast);
    }

    public function test_alter_view_rename_if_exists_sets_flag() : void
    {
        $builder = AlterViewBuilder::create('old_view')
            ->ifExists()
            ->renameTo('new_view');

        $ast = $builder->toAst();

        self::assertTrue($ast->getMissingOk());
    }

    public function test_alter_view_set_schema_ast_type() : void
    {
        $builder = AlterViewBuilder::create('my_view')
            ->setSchema('archive');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterObjectSchemaStmt::class, $ast);
        self::assertSame('archive', $ast->getNewschema());
    }

    public function test_create_materialized_view_ast_type() : void
    {
        $builder = CreateMaterializedViewBuilder::create('my_matview')
            ->as(SelectBuilder::create()->select(star())->from(table('users')));

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateTableAsStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_MATVIEW, $ast->getObjtype());
    }

    public function test_create_materialized_view_if_not_exists_sets_flag() : void
    {
        $builder = CreateMaterializedViewBuilder::create('my_matview')
            ->ifNotExists()
            ->as(SelectBuilder::create()->select(star())->from(table('users')));

        $ast = $builder->toAst();

        self::assertTrue($ast->getIfNotExists());
    }

    public function test_create_materialized_view_parses_schema_from_name() : void
    {
        $builder = CreateMaterializedViewBuilder::create('analytics.my_matview')
            ->as(SelectBuilder::create()->select(star())->from(table('users')));

        $ast = $builder->toAst();
        $into = $ast->getInto();

        self::assertNotNull($into);
        $rel = $into->getRel();
        self::assertNotNull($rel);
        self::assertSame('analytics', $rel->getSchemaname());
        self::assertSame('my_matview', $rel->getRelname());
    }

    public function test_create_materialized_view_with_columns() : void
    {
        $builder = CreateMaterializedViewBuilder::create('my_matview')
            ->columns('user_id', 'total_orders')
            ->as(SelectBuilder::create()->select(star())->from(table('users')));

        $ast = $builder->toAst();
        $into = $ast->getInto();

        self::assertNotNull($into);
        self::assertCount(2, $into->getColNames());
    }

    public function test_create_materialized_view_with_no_data_sets_flag() : void
    {
        $builder = CreateMaterializedViewBuilder::create('my_matview')
            ->as(SelectBuilder::create()->select(star())->from(table('users')))
            ->withNoData();

        $ast = $builder->toAst();
        $into = $ast->getInto();

        self::assertNotNull($into);
        self::assertTrue($into->getSkipData());
    }

    public function test_create_materialized_view_with_tablespace() : void
    {
        $builder = CreateMaterializedViewBuilder::create('my_matview')
            ->as(SelectBuilder::create()->select(star())->from(table('users')))
            ->tablespace('fast_storage');

        $ast = $builder->toAst();
        $into = $ast->getInto();

        self::assertNotNull($into);
        self::assertSame('fast_storage', $into->getTableSpaceName());
    }

    public function test_create_view_ast_type() : void
    {
        $builder = CreateViewBuilder::create('my_view')
            ->as(SelectBuilder::create()->select(star())->from(table('users')));

        $ast = $builder->toAst();

        self::assertInstanceOf(ViewStmt::class, $ast);
    }

    public function test_create_view_immutability() : void
    {
        $original = CreateViewBuilder::create('my_view');
        $modified = $original->orReplace();

        $originalAst = $original->as(SelectBuilder::create()->select(star())->from(table('users')))->toAst();
        $modifiedAst = $modified->as(SelectBuilder::create()->select(star())->from(table('users')))->toAst();

        self::assertFalse($originalAst->getReplace());
        self::assertTrue($modifiedAst->getReplace());
    }

    public function test_create_view_or_replace_sets_flag() : void
    {
        $builder = CreateViewBuilder::create('my_view')
            ->orReplace()
            ->as(SelectBuilder::create()->select(star())->from(table('users')));

        $ast = $builder->toAst();

        self::assertTrue($ast->getReplace());
    }

    public function test_create_view_parses_schema_from_name() : void
    {
        $builder = CreateViewBuilder::create('public.my_view')
            ->as(SelectBuilder::create()->select(star())->from(table('users')));

        $ast = $builder->toAst();
        $view = $ast->getView();

        self::assertNotNull($view);
        self::assertSame('public', $view->getSchemaname());
        self::assertSame('my_view', $view->getRelname());
    }

    public function test_create_view_with_columns() : void
    {
        $builder = CreateViewBuilder::create('my_view')
            ->columns('id', 'name', 'email')
            ->as(SelectBuilder::create()->select(star())->from(table('users')));

        $ast = $builder->toAst();

        self::assertCount(3, $ast->getAliases());
    }

    public function test_drop_materialized_view_ast_type() : void
    {
        $builder = DropMaterializedViewBuilder::create('my_matview');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_MATVIEW, $ast->getRemoveType());
    }

    public function test_drop_view_ast_type() : void
    {
        $builder = DropViewBuilder::create('my_view');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_VIEW, $ast->getRemoveType());
    }

    public function test_drop_view_if_exists_sets_flag() : void
    {
        $builder = DropViewBuilder::create('my_view')
            ->ifExists();

        $ast = $builder->toAst();

        self::assertTrue($ast->getMissingOk());
    }

    public function test_drop_view_multiple_views() : void
    {
        $builder = DropViewBuilder::create('view1', 'view2', 'view3');

        $ast = $builder->toAst();

        self::assertCount(3, $ast->getObjects());
    }

    public function test_refresh_materialized_view_ast_type() : void
    {
        $builder = RefreshMaterializedViewBuilder::create('my_matview');

        $ast = $builder->toAst();

        self::assertInstanceOf(RefreshMatViewStmt::class, $ast);
    }

    public function test_refresh_materialized_view_concurrently_sets_flag() : void
    {
        $builder = RefreshMaterializedViewBuilder::create('my_matview')
            ->concurrently();

        $ast = $builder->toAst();

        self::assertTrue($ast->getConcurrent());
    }

    public function test_refresh_materialized_view_parses_schema_from_name() : void
    {
        $builder = RefreshMaterializedViewBuilder::create('analytics.my_matview');

        $ast = $builder->toAst();
        $relation = $ast->getRelation();

        self::assertNotNull($relation);
        self::assertSame('analytics', $relation->getSchemaname());
        self::assertSame('my_matview', $relation->getRelname());
    }

    public function test_refresh_materialized_view_with_no_data_sets_flag() : void
    {
        $builder = RefreshMaterializedViewBuilder::create('my_matview')
            ->withNoData();

        $ast = $builder->toAst();

        self::assertTrue($ast->getSkipData());
    }
}
