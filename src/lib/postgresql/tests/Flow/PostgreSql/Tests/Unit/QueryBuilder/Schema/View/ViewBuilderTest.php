<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\View;

use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\Protobuf\AST\CreateTableAsStmt;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\RefreshMatViewStmt;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\Protobuf\AST\ViewStmt;
use Flow\PostgreSql\QueryBuilder\Schema\View\AlterMaterializedView\AlterMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\AlterView\AlterViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\CreateMaterializedView\CreateMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\CreateView\CreateViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\DropMaterializedView\DropMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\DropView\DropViewBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\View\RefreshMaterializedView\RefreshMaterializedViewBuilder;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\refresh_materialized_view;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;

final class ViewBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_materialized_view_owner_to_to_sql(): void
    {
        static::assertSame(
            'ALTER MATERIALIZED VIEW my_matview OWNER TO new_owner',
            alter()->materializedView('my_matview')->ownerTo('new_owner')->toSql(),
        );
    }

    public function test_alter_materialized_view_rename_if_exists_to_sql(): void
    {
        static::assertSame(
            'ALTER MATERIALIZED VIEW IF EXISTS old_matview RENAME TO new_matview',
            alter()->materializedView('old_matview')->ifExists()->renameTo('new_matview')->toSql(),
        );
    }

    public function test_alter_materialized_view_rename_to_sql(): void
    {
        static::assertSame(
            'ALTER MATERIALIZED VIEW old_matview RENAME TO new_matview',
            alter()->materializedView('old_matview')->renameTo('new_matview')->toSql(),
        );
    }

    public function test_alter_materialized_view_set_schema_to_sql(): void
    {
        static::assertSame(
            'ALTER MATERIALIZED VIEW my_matview SET SCHEMA archive',
            alter()->materializedView('my_matview')->setSchema('archive')->toSql(),
        );
    }

    public function test_alter_materialized_view_set_tablespace_ast_type(): void
    {
        $builder = AlterMaterializedViewBuilder::create('my_matview')->setTablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
    }

    public function test_alter_materialized_view_set_tablespace_if_exists_sets_flag(): void
    {
        $builder = AlterMaterializedViewBuilder::create('my_matview')->ifExists()->setTablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertTrue($ast->getMissingOk());
    }

    public function test_alter_materialized_view_set_tablespace_if_exists_to_sql(): void
    {
        static::assertSame(
            'ALTER MATERIALIZED VIEW IF EXISTS my_matview SET TABLESPACE fast_storage',
            alter()->materializedView('my_matview')->ifExists()->setTablespace('fast_storage')->toSql(),
        );
    }

    public function test_alter_materialized_view_set_tablespace_to_sql(): void
    {
        static::assertSame(
            'ALTER MATERIALIZED VIEW my_matview SET TABLESPACE fast_storage',
            alter()->materializedView('my_matview')->setTablespace('fast_storage')->toSql(),
        );
    }

    public function test_alter_view_owner_to_ast_type(): void
    {
        $builder = AlterViewBuilder::create('my_view')->ownerTo('new_owner');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_VIEW, $ast->getObjtype());
    }

    public function test_alter_view_owner_to_to_sql(): void
    {
        static::assertSame(
            'ALTER VIEW my_view OWNER TO new_owner',
            alter()->view('my_view')->ownerTo('new_owner')->toSql(),
        );
    }

    public function test_alter_view_rename_ast_type(): void
    {
        $builder = AlterViewBuilder::create('old_view')->renameTo('new_view');

        $ast = $builder->toAst();

        static::assertInstanceOf(RenameStmt::class, $ast);
    }

    public function test_alter_view_rename_if_exists_sets_flag(): void
    {
        $builder = AlterViewBuilder::create('old_view')->ifExists()->renameTo('new_view');

        $ast = $builder->toAst();

        static::assertTrue($ast->getMissingOk());
    }

    public function test_alter_view_rename_if_exists_to_sql(): void
    {
        static::assertSame(
            'ALTER VIEW IF EXISTS old_view RENAME TO new_view',
            alter()->view('old_view')->ifExists()->renameTo('new_view')->toSql(),
        );
    }

    public function test_alter_view_rename_to_sql(): void
    {
        static::assertSame(
            'ALTER VIEW old_view RENAME TO new_view',
            alter()->view('old_view')->renameTo('new_view')->toSql(),
        );
    }

    public function test_alter_view_rename_with_schema_to_sql(): void
    {
        static::assertSame(
            'ALTER VIEW public.old_view RENAME TO new_view',
            alter()->view('public.old_view')->renameTo('new_view')->toSql(),
        );
    }

    public function test_alter_view_set_schema_ast_type(): void
    {
        $builder = AlterViewBuilder::create('my_view')->setSchema('archive');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterObjectSchemaStmt::class, $ast);
        static::assertSame('archive', $ast->getNewschema());
    }

    public function test_alter_view_set_schema_if_exists_to_sql(): void
    {
        static::assertSame(
            'ALTER VIEW IF EXISTS my_view SET SCHEMA archive',
            alter()->view('my_view')->ifExists()->setSchema('archive')->toSql(),
        );
    }

    public function test_alter_view_set_schema_to_sql(): void
    {
        static::assertSame(
            'ALTER VIEW my_view SET SCHEMA archive',
            alter()->view('my_view')->setSchema('archive')->toSql(),
        );
    }

    public function test_create_materialized_view_ast_type(): void
    {
        $builder = CreateMaterializedViewBuilder::create('my_matview')->as(
            SelectBuilder::create()->select(star())->from(table('users')),
        );

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateTableAsStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_MATVIEW, $ast->getObjtype());
    }

    public function test_create_materialized_view_if_not_exists_sets_flag(): void
    {
        $builder = CreateMaterializedViewBuilder::create('my_matview')->ifNotExists()->as(
            SelectBuilder::create()->select(star())->from(table('users')),
        );

        $ast = $builder->toAst();

        static::assertTrue($ast->getIfNotExists());
    }

    public function test_create_materialized_view_if_not_exists_to_sql(): void
    {
        static::assertSame(
            'CREATE MATERIALIZED VIEW IF NOT EXISTS user_stats AS SELECT * FROM users',
            create()
                ->materializedView('user_stats')
                ->ifNotExists()
                ->as(select(star())->from(table('users')))
                ->toSql(),
        );
    }

    public function test_create_materialized_view_parses_schema_from_name(): void
    {
        $builder = CreateMaterializedViewBuilder::create('analytics.my_matview')->as(
            SelectBuilder::create()->select(star())->from(table('users')),
        );

        $ast = $builder->toAst();
        $into = $ast->getInto();

        static::assertNotNull($into);
        $rel = $into->getRel();
        static::assertNotNull($rel);
        static::assertSame('analytics', $rel->getSchemaname());
        static::assertSame('my_matview', $rel->getRelname());
    }

    public function test_create_materialized_view_to_sql(): void
    {
        static::assertSame(
            'CREATE MATERIALIZED VIEW user_stats AS SELECT * FROM users',
            create()
                ->materializedView('user_stats')
                ->as(select(star())->from(table('users')))
                ->toSql(),
        );
    }

    public function test_create_materialized_view_using_access_method_to_sql(): void
    {
        static::assertSame(
            'CREATE MATERIALIZED VIEW user_stats USING heap AS SELECT * FROM users',
            create()
                ->materializedView('user_stats')
                ->using('heap')
                ->as(select(star())->from(table('users')))
                ->toSql(),
        );
    }

    public function test_create_materialized_view_with_columns(): void
    {
        $builder = CreateMaterializedViewBuilder::create('my_matview')->columns('user_id', 'total_orders')->as(
            SelectBuilder::create()->select(star())->from(table('users')),
        );

        $ast = $builder->toAst();
        $into = $ast->getInto();

        static::assertNotNull($into);
        static::assertCount(2, $into->getColNames());
    }

    public function test_create_materialized_view_with_data_to_sql(): void
    {
        static::assertSame(
            'CREATE MATERIALIZED VIEW user_stats AS SELECT * FROM users',
            create()
                ->materializedView('user_stats')
                ->as(select()->select(star())->from(table('users')))
                ->withData()
                ->toSql(),
        );
    }

    public function test_create_materialized_view_with_no_data_sets_flag(): void
    {
        $builder = CreateMaterializedViewBuilder::create('my_matview')->as(
            SelectBuilder::create()->select(star())->from(table('users')),
        )->withNoData();

        $ast = $builder->toAst();
        $into = $ast->getInto();

        static::assertNotNull($into);
        static::assertTrue($into->getSkipData());
    }

    public function test_create_materialized_view_with_no_data_to_sql(): void
    {
        static::assertSame(
            'CREATE MATERIALIZED VIEW user_stats AS SELECT * FROM users  WITH NO DATA',
            create()
                ->materializedView('user_stats')
                ->as(select()->select(star())->from(table('users')))
                ->withNoData()
                ->toSql(),
        );
    }

    public function test_create_materialized_view_with_schema_to_sql(): void
    {
        static::assertSame(
            'CREATE MATERIALIZED VIEW analytics.user_stats AS SELECT * FROM users',
            create()
                ->materializedView('analytics.user_stats')
                ->as(select(star())->from(table('users')))
                ->toSql(),
        );
    }

    public function test_create_materialized_view_with_tablespace(): void
    {
        $builder = CreateMaterializedViewBuilder::create('my_matview')->as(
            SelectBuilder::create()->select(star())->from(table('users')),
        )->tablespace('fast_storage');

        $ast = $builder->toAst();
        $into = $ast->getInto();

        static::assertNotNull($into);
        static::assertSame('fast_storage', $into->getTableSpaceName());
    }

    public function test_create_or_replace_view_to_sql(): void
    {
        static::assertSame(
            'CREATE OR REPLACE VIEW active_users AS SELECT * FROM users',
            create()
                ->view('active_users')
                ->orReplace()
                ->as(select(star())->from(table('users')))
                ->toSql(),
        );
    }

    public function test_create_recursive_view_outputs_as_regular_view_to_sql(): void
    {
        static::assertSame(
            'CREATE VIEW subordinates (id, name, manager_id) AS SELECT id, name, manager_id FROM employees',
            create()
                ->view('subordinates')
                ->recursive()
                ->columns('id', 'name', 'manager_id')
                ->as(select(col('id'), col('name'), col('manager_id'))->from(table('employees')))
                ->toSql(),
        );
    }

    public function test_create_temporary_view_to_sql(): void
    {
        static::assertSame(
            'CREATE TEMPORARY VIEW temp_users AS SELECT * FROM users',
            create()
                ->view('temp_users')
                ->temporary()
                ->as(select(star())->from(table('users')))
                ->toSql(),
        );
    }

    public function test_create_view_ast_type(): void
    {
        $builder = CreateViewBuilder::create('my_view')->as(
            SelectBuilder::create()->select(star())->from(table('users')),
        );

        $ast = $builder->toAst();

        static::assertInstanceOf(ViewStmt::class, $ast);
    }

    public function test_create_view_immutability(): void
    {
        $original = CreateViewBuilder::create('my_view');
        $modified = $original->orReplace();

        $originalAst = $original->as(SelectBuilder::create()->select(star())->from(table('users')))->toAst();
        $modifiedAst = $modified->as(SelectBuilder::create()->select(star())->from(table('users')))->toAst();

        static::assertFalse($originalAst->getReplace());
        static::assertTrue($modifiedAst->getReplace());
    }

    public function test_create_view_or_replace_sets_flag(): void
    {
        $builder = CreateViewBuilder::create('my_view')->orReplace()->as(
            SelectBuilder::create()->select(star())->from(table('users')),
        );

        $ast = $builder->toAst();

        static::assertTrue($ast->getReplace());
    }

    public function test_create_view_parses_schema_from_name(): void
    {
        $builder = CreateViewBuilder::create('public.my_view')->as(
            SelectBuilder::create()->select(star())->from(table('users')),
        );

        $ast = $builder->toAst();
        $view = $ast->getView();

        static::assertNotNull($view);
        static::assertSame('public', $view->getSchemaname());
        static::assertSame('my_view', $view->getRelname());
    }

    public function test_create_view_simple_to_sql(): void
    {
        static::assertSame(
            'CREATE VIEW active_users AS SELECT * FROM users',
            create()
                ->view('active_users')
                ->as(select(star())->from(table('users')))
                ->toSql(),
        );
    }

    public function test_create_view_with_cascaded_check_option_to_sql(): void
    {
        static::assertSame(
            'CREATE VIEW active_users AS SELECT * FROM users WITH CHECK OPTION',
            create()
                ->view('active_users')
                ->as(select()->select(star())->from(table('users')))
                ->withCascadedCheckOption()
                ->toSql(),
        );
    }

    public function test_create_view_with_check_option_to_sql(): void
    {
        static::assertSame(
            'CREATE VIEW active_users AS SELECT * FROM users WHERE active = true WITH CHECK OPTION',
            create()
                ->view('active_users')
                ->as(select(star())->from(table('users'))->where(eq(col('active'), literal(true))))
                ->withCheckOption()
                ->toSql(),
        );
    }

    public function test_create_view_with_columns(): void
    {
        $builder = CreateViewBuilder::create('my_view')->columns('id', 'name', 'email')->as(
            SelectBuilder::create()->select(star())->from(table('users')),
        );

        $ast = $builder->toAst();

        static::assertCount(3, $ast->getAliases());
    }

    public function test_create_view_with_local_check_option_to_sql(): void
    {
        static::assertSame(
            'CREATE VIEW active_users AS SELECT * FROM users WITH LOCAL CHECK OPTION',
            create()
                ->view('active_users')
                ->as(select()->select(star())->from(table('users')))
                ->withLocalCheckOption()
                ->toSql(),
        );
    }

    public function test_create_view_with_schema_to_sql(): void
    {
        static::assertSame(
            'CREATE VIEW public.active_users AS SELECT * FROM users',
            create()
                ->view('public.active_users')
                ->as(select(star())->from(table('users')))
                ->toSql(),
        );
    }

    public function test_drop_materialized_view_ast_type(): void
    {
        $builder = DropMaterializedViewBuilder::create('my_matview');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_MATVIEW, $ast->getRemoveType());
    }

    public function test_drop_materialized_view_if_exists_cascade_to_sql(): void
    {
        static::assertSame(
            'DROP MATERIALIZED VIEW IF EXISTS user_stats CASCADE',
            drop()->materializedView('user_stats')->ifExists()->cascade()->toSql(),
        );
    }

    public function test_drop_materialized_view_simple_to_sql(): void
    {
        static::assertSame('DROP MATERIALIZED VIEW user_stats', drop()->materializedView('user_stats')->toSql());
    }

    public function test_drop_view_ast_type(): void
    {
        $builder = DropViewBuilder::create('my_view');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_VIEW, $ast->getRemoveType());
    }

    public function test_drop_view_cascade_to_sql(): void
    {
        static::assertSame('DROP VIEW active_users CASCADE', drop()->view('active_users')->cascade()->toSql());
    }

    public function test_drop_view_if_exists_cascade_to_sql(): void
    {
        static::assertSame(
            'DROP VIEW IF EXISTS active_users CASCADE',
            drop()->view('active_users')->ifExists()->cascade()->toSql(),
        );
    }

    public function test_drop_view_if_exists_sets_flag(): void
    {
        $builder = DropViewBuilder::create('my_view')->ifExists();

        $ast = $builder->toAst();

        static::assertTrue($ast->getMissingOk());
    }

    public function test_drop_view_if_exists_to_sql(): void
    {
        static::assertSame('DROP VIEW IF EXISTS active_users', drop()->view('active_users')->ifExists()->toSql());
    }

    public function test_drop_view_multiple_to_sql(): void
    {
        static::assertSame('DROP VIEW view1, view2, view3', drop()->view('view1', 'view2', 'view3')->toSql());
    }

    public function test_drop_view_multiple_views(): void
    {
        $builder = DropViewBuilder::create('view1', 'view2', 'view3');

        $ast = $builder->toAst();

        static::assertCount(3, $ast->getObjects());
    }

    public function test_drop_view_simple_to_sql(): void
    {
        static::assertSame('DROP VIEW active_users', drop()->view('active_users')->toSql());
    }

    public function test_refresh_materialized_view_ast_type(): void
    {
        $builder = RefreshMaterializedViewBuilder::create('my_matview');

        $ast = $builder->toAst();

        static::assertInstanceOf(RefreshMatViewStmt::class, $ast);
    }

    public function test_refresh_materialized_view_concurrently_sets_flag(): void
    {
        $builder = RefreshMaterializedViewBuilder::create('my_matview')->concurrently();

        $ast = $builder->toAst();

        static::assertTrue($ast->getConcurrent());
    }

    public function test_refresh_materialized_view_concurrently_to_sql(): void
    {
        static::assertSame(
            'REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats',
            refresh_materialized_view('user_stats')->concurrently()->toSql(),
        );
    }

    public function test_refresh_materialized_view_concurrently_with_data_to_sql(): void
    {
        static::assertSame(
            'REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats',
            refresh_materialized_view('user_stats')->concurrently()->withData()->toSql(),
        );
    }

    public function test_refresh_materialized_view_parses_schema_from_name(): void
    {
        $builder = RefreshMaterializedViewBuilder::create('analytics.my_matview');

        $ast = $builder->toAst();
        $relation = $ast->getRelation();

        static::assertNotNull($relation);
        static::assertSame('analytics', $relation->getSchemaname());
        static::assertSame('my_matview', $relation->getRelname());
    }

    public function test_refresh_materialized_view_simple_to_sql(): void
    {
        static::assertSame('REFRESH MATERIALIZED VIEW user_stats', refresh_materialized_view('user_stats')->toSql());
    }

    public function test_refresh_materialized_view_with_data_to_sql(): void
    {
        static::assertSame(
            'REFRESH MATERIALIZED VIEW user_stats',
            refresh_materialized_view('user_stats')->withData()->toSql(),
        );
    }

    public function test_refresh_materialized_view_with_no_data_sets_flag(): void
    {
        $builder = RefreshMaterializedViewBuilder::create('my_matview')->withNoData();

        $ast = $builder->toAst();

        static::assertTrue($ast->getSkipData());
    }

    public function test_refresh_materialized_view_with_no_data_to_sql(): void
    {
        static::assertSame(
            'REFRESH MATERIALIZED VIEW user_stats WITH NO DATA',
            refresh_materialized_view('user_stats')->withNoData()->toSql(),
        );
    }

    public function test_refresh_materialized_view_with_schema_to_sql(): void
    {
        static::assertSame(
            'REFRESH MATERIALIZED VIEW analytics.user_stats',
            refresh_materialized_view('analytics.user_stats')->toSql(),
        );
    }
}
