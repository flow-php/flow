<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    agg_count,
    alter_materialized_view,
    alter_view,
    col,
    create_materialized_view,
    create_view,
    drop_materialized_view,
    drop_view,
    eq,
    literal_bool,
    refresh_materialized_view,
    select,
    star,
    table
};

final class ViewBuilderTest extends PGQueryTestCase
{
    public function test_alter_materialized_view_owner_to() : void
    {
        $builder = alter_materialized_view('my_matview')
            ->ownerTo('new_owner');

        $this->assertAlterMaterializedViewOwnerQuery(
            $builder,
            'ALTER MATERIALIZED VIEW my_matview OWNER TO new_owner'
        );
    }

    public function test_alter_materialized_view_rename() : void
    {
        $builder = alter_materialized_view('old_matview')
            ->renameTo('new_matview');

        $this->assertAlterMaterializedViewRenameQuery(
            $builder,
            'ALTER MATERIALIZED VIEW old_matview RENAME TO new_matview'
        );
    }

    public function test_alter_materialized_view_rename_if_exists() : void
    {
        $builder = alter_materialized_view('old_matview')
            ->ifExists()
            ->renameTo('new_matview');

        $this->assertAlterMaterializedViewRenameQuery(
            $builder,
            'ALTER MATERIALIZED VIEW IF EXISTS old_matview RENAME TO new_matview'
        );
    }

    public function test_alter_materialized_view_set_schema() : void
    {
        $builder = alter_materialized_view('my_matview')
            ->setSchema('archive');

        $this->assertAlterMaterializedViewSchemaQuery(
            $builder,
            'ALTER MATERIALIZED VIEW my_matview SET SCHEMA archive'
        );
    }

    public function test_alter_materialized_view_set_tablespace() : void
    {
        $builder = alter_materialized_view('my_matview')
            ->setTablespace('fast_storage');

        $this->assertAlterMaterializedViewTablespaceQuery(
            $builder,
            'ALTER MATERIALIZED VIEW my_matview SET TABLESPACE fast_storage'
        );
    }

    public function test_alter_materialized_view_set_tablespace_if_exists() : void
    {
        $builder = alter_materialized_view('my_matview')
            ->ifExists()
            ->setTablespace('fast_storage');

        $this->assertAlterMaterializedViewTablespaceQuery(
            $builder,
            'ALTER MATERIALIZED VIEW IF EXISTS my_matview SET TABLESPACE fast_storage'
        );
    }

    public function test_alter_view_owner_to() : void
    {
        $builder = alter_view('my_view')
            ->ownerTo('new_owner');

        $this->assertAlterViewOwnerQuery(
            $builder,
            'ALTER VIEW my_view OWNER TO new_owner'
        );
    }

    public function test_alter_view_rename() : void
    {
        $builder = alter_view('old_view')
            ->renameTo('new_view');

        $this->assertAlterViewRenameQuery(
            $builder,
            'ALTER VIEW old_view RENAME TO new_view'
        );
    }

    public function test_alter_view_rename_if_exists() : void
    {
        $builder = alter_view('old_view')
            ->ifExists()
            ->renameTo('new_view');

        $this->assertAlterViewRenameQuery(
            $builder,
            'ALTER VIEW IF EXISTS old_view RENAME TO new_view'
        );
    }

    public function test_alter_view_rename_with_schema() : void
    {
        $builder = alter_view('public.old_view')
            ->renameTo('new_view');

        $this->assertAlterViewRenameQuery(
            $builder,
            'ALTER VIEW public.old_view RENAME TO new_view'
        );
    }

    public function test_alter_view_set_schema() : void
    {
        $builder = alter_view('my_view')
            ->setSchema('archive');

        $this->assertAlterViewSchemaQuery(
            $builder,
            'ALTER VIEW my_view SET SCHEMA archive'
        );
    }

    public function test_alter_view_set_schema_if_exists() : void
    {
        $builder = alter_view('my_view')
            ->ifExists()
            ->setSchema('archive');

        $this->assertAlterViewSchemaQuery(
            $builder,
            'ALTER VIEW IF EXISTS my_view SET SCHEMA archive'
        );
    }

    public function test_create_materialized_view() : void
    {
        $builder = create_materialized_view('user_stats')
            ->as(select()->select(star())->from(table('users')));

        $this->assertCreateMaterializedViewQuery(
            $builder,
            'CREATE MATERIALIZED VIEW user_stats AS SELECT * FROM users'
        );
    }

    public function test_create_materialized_view_if_not_exists() : void
    {
        $builder = create_materialized_view('user_stats')
            ->ifNotExists()
            ->as(select()->select(star())->from(table('users')));

        $this->assertCreateMaterializedViewQuery(
            $builder,
            'CREATE MATERIALIZED VIEW IF NOT EXISTS user_stats AS SELECT * FROM users'
        );
    }

    public function test_create_materialized_view_using_access_method() : void
    {
        $builder = create_materialized_view('user_stats')
            ->using('heap')
            ->as(select()->select(star())->from(table('users')));

        $this->assertCreateMaterializedViewQuery(
            $builder,
            'CREATE MATERIALIZED VIEW user_stats USING heap AS SELECT * FROM users'
        );
    }

    public function test_create_materialized_view_with_columns() : void
    {
        $builder = create_materialized_view('user_stats')
            ->columns('user_id', 'order_count')
            ->as(select()->select(col('id'), agg_count())->from(table('users')));

        $this->assertCreateMaterializedViewQuery(
            $builder,
            'CREATE MATERIALIZED VIEW user_stats(user_id, order_count) AS SELECT id, count(*) FROM users'
        );
    }

    public function test_create_materialized_view_with_data() : void
    {
        $builder = create_materialized_view('user_stats')
            ->as(select()->select(star())->from(table('users')))
            ->withData();

        $this->assertCreateMaterializedViewQuery(
            $builder,
            'CREATE MATERIALIZED VIEW user_stats AS SELECT * FROM users'
        );
    }

    public function test_create_materialized_view_with_no_data() : void
    {
        $builder = create_materialized_view('user_stats')
            ->as(select()->select(star())->from(table('users')))
            ->withNoData();

        $this->assertCreateMaterializedViewQuery(
            $builder,
            'CREATE MATERIALIZED VIEW user_stats AS SELECT * FROM users  WITH NO DATA'
        );
    }

    public function test_create_materialized_view_with_schema() : void
    {
        $builder = create_materialized_view('analytics.user_stats')
            ->as(select()->select(star())->from(table('users')));

        $this->assertCreateMaterializedViewQuery(
            $builder,
            'CREATE MATERIALIZED VIEW analytics.user_stats AS SELECT * FROM users'
        );
    }

    public function test_create_materialized_view_with_tablespace() : void
    {
        $builder = create_materialized_view('user_stats')
            ->as(select()->select(star())->from(table('users')))
            ->tablespace('fast_storage');

        $this->assertCreateMaterializedViewQuery(
            $builder,
            'CREATE MATERIALIZED VIEW user_stats TABLESPACE fast_storage AS SELECT * FROM users'
        );
    }

    public function test_create_or_replace_view() : void
    {
        $builder = create_view('active_users')
            ->orReplace()
            ->as(select()->select(star())->from(table('users')));

        $this->assertCreateViewQuery(
            $builder,
            'CREATE OR REPLACE VIEW active_users AS SELECT * FROM users'
        );
    }

    /**
     * Note: CREATE RECURSIVE VIEW is syntactic sugar that PostgreSQL internally transforms
     * into a CTE-based query structure. The pg_query deparser has issues handling this
     * transformed AST (segfaults), so this feature is not fully supported in the query builder.
     * Use a regular view with a CTE instead for recursive queries.
     */
    public function test_create_recursive_view_outputs_as_regular_view() : void
    {
        $builder = create_view('subordinates')
            ->recursive()
            ->columns('id', 'name', 'manager_id')
            ->as(select()->select(col('id'), col('name'), col('manager_id'))->from(table('employees')));

        $this->assertCreateViewQuery(
            $builder,
            'CREATE VIEW subordinates (id, name, manager_id) AS SELECT id, name, manager_id FROM employees'
        );
    }

    public function test_create_temporary_view() : void
    {
        $builder = create_view('temp_users')
            ->temporary()
            ->as(select()->select(star())->from(table('users')));

        $this->assertCreateViewQuery(
            $builder,
            'CREATE TEMPORARY VIEW temp_users AS SELECT * FROM users'
        );
    }

    public function test_create_view_simple() : void
    {
        $builder = create_view('active_users')
            ->as(select()->select(star())->from(table('users')));

        $this->assertCreateViewQuery(
            $builder,
            'CREATE VIEW active_users AS SELECT * FROM users'
        );
    }

    public function test_create_view_with_cascaded_check_option() : void
    {
        $builder = create_view('active_users')
            ->as(select()->select(star())->from(table('users')))
            ->withCascadedCheckOption();

        $this->assertCreateViewQuery(
            $builder,
            'CREATE VIEW active_users AS SELECT * FROM users WITH CHECK OPTION'
        );
    }

    public function test_create_view_with_check_option() : void
    {
        $builder = create_view('active_users')
            ->as(select()->select(star())->from(table('users'))->where(eq(col('active'), literal_bool(true))))
            ->withCheckOption();

        $this->assertCreateViewQuery(
            $builder,
            'CREATE VIEW active_users AS SELECT * FROM users WHERE active = true WITH CHECK OPTION'
        );
    }

    public function test_create_view_with_columns() : void
    {
        $builder = create_view('user_info')
            ->columns('user_id', 'user_name', 'email_address')
            ->as(select()->select(col('id'), col('name'), col('email'))->from(table('users')));

        $this->assertCreateViewQuery(
            $builder,
            'CREATE VIEW user_info (user_id, user_name, email_address) AS SELECT id, name, email FROM users'
        );
    }

    public function test_create_view_with_local_check_option() : void
    {
        $builder = create_view('active_users')
            ->as(select()->select(star())->from(table('users')))
            ->withLocalCheckOption();

        $this->assertCreateViewQuery(
            $builder,
            'CREATE VIEW active_users AS SELECT * FROM users WITH LOCAL CHECK OPTION'
        );
    }

    public function test_create_view_with_schema() : void
    {
        $builder = create_view('public.active_users')
            ->as(select()->select(star())->from(table('users')));

        $this->assertCreateViewQuery(
            $builder,
            'CREATE VIEW public.active_users AS SELECT * FROM users'
        );
    }

    public function test_drop_materialized_view_if_exists_cascade() : void
    {
        $builder = drop_materialized_view('user_stats')
            ->ifExists()
            ->cascade();

        $this->assertDropMaterializedViewQuery(
            $builder,
            'DROP MATERIALIZED VIEW IF EXISTS user_stats CASCADE'
        );
    }

    public function test_drop_materialized_view_simple() : void
    {
        $builder = drop_materialized_view('user_stats');

        $this->assertDropMaterializedViewQuery(
            $builder,
            'DROP MATERIALIZED VIEW user_stats'
        );
    }

    public function test_drop_view_cascade() : void
    {
        $builder = drop_view('active_users')->cascade();

        $this->assertDropViewQuery(
            $builder,
            'DROP VIEW active_users CASCADE'
        );
    }

    public function test_drop_view_if_exists() : void
    {
        $builder = drop_view('active_users')->ifExists();

        $this->assertDropViewQuery(
            $builder,
            'DROP VIEW IF EXISTS active_users'
        );
    }

    public function test_drop_view_if_exists_cascade() : void
    {
        $builder = drop_view('active_users')
            ->ifExists()
            ->cascade();

        $this->assertDropViewQuery(
            $builder,
            'DROP VIEW IF EXISTS active_users CASCADE'
        );
    }

    public function test_drop_view_multiple() : void
    {
        $builder = drop_view('view1', 'view2', 'view3');

        $this->assertDropViewQuery(
            $builder,
            'DROP VIEW view1, view2, view3'
        );
    }

    public function test_drop_view_simple() : void
    {
        $builder = drop_view('active_users');

        $this->assertDropViewQuery(
            $builder,
            'DROP VIEW active_users'
        );
    }

    public function test_refresh_materialized_view_concurrently() : void
    {
        $builder = refresh_materialized_view('user_stats')
            ->concurrently();

        $this->assertRefreshMaterializedViewQuery(
            $builder,
            'REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats'
        );
    }

    public function test_refresh_materialized_view_concurrently_with_data() : void
    {
        $builder = refresh_materialized_view('user_stats')
            ->concurrently()
            ->withData();

        $this->assertRefreshMaterializedViewQuery(
            $builder,
            'REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats'
        );
    }

    public function test_refresh_materialized_view_simple() : void
    {
        $builder = refresh_materialized_view('user_stats');

        $this->assertRefreshMaterializedViewQuery(
            $builder,
            'REFRESH MATERIALIZED VIEW user_stats'
        );
    }

    public function test_refresh_materialized_view_with_data() : void
    {
        $builder = refresh_materialized_view('user_stats')
            ->withData();

        $this->assertRefreshMaterializedViewQuery(
            $builder,
            'REFRESH MATERIALIZED VIEW user_stats'
        );
    }

    public function test_refresh_materialized_view_with_no_data() : void
    {
        $builder = refresh_materialized_view('user_stats')
            ->withNoData();

        $this->assertRefreshMaterializedViewQuery(
            $builder,
            'REFRESH MATERIALIZED VIEW user_stats WITH NO DATA'
        );
    }

    public function test_refresh_materialized_view_with_schema() : void
    {
        $builder = refresh_materialized_view('analytics.user_stats');

        $this->assertRefreshMaterializedViewQuery(
            $builder,
            'REFRESH MATERIALIZED VIEW analytics.user_stats'
        );
    }
}
