<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    analyze,
    analyze_table,
    cluster,
    cluster_table,
    comment,
    discard,
    explain,
    explain_analyze,
    lock_table,
    select,
    star,
    table,
    vacuum,
    vacuum_analyze,
    vacuum_full
};
use Flow\PgQuery\QueryBuilder\Utility\{CommentTarget, DiscardType, ExplainFormat, IndexCleanup, LockMode};

final class UtilityBuilderTest extends PGQueryTestCase
{
    public function test_analyze_all_tables() : void
    {
        $builder = analyze();

        $this->assertAnalyzeQuery(
            $builder,
            'ANALYZE'
        );
    }

    public function test_analyze_multiple_tables() : void
    {
        $builder = analyze()->tables('users', 'orders', 'products');

        $this->assertAnalyzeQuery(
            $builder,
            'ANALYZE users, orders, products'
        );
    }

    public function test_analyze_single_table() : void
    {
        $builder = analyze_table('users');

        $this->assertAnalyzeQuery(
            $builder,
            'ANALYZE users'
        );
    }

    public function test_analyze_skip_locked() : void
    {
        $builder = analyze()->skipLocked()->table('users');

        $this->assertAnalyzeQuery(
            $builder,
            'ANALYZE (SKIP_LOCKED) users'
        );
    }

    public function test_analyze_table_with_columns() : void
    {
        $builder = analyze_table('users', 'email', 'name');

        $this->assertAnalyzeQuery(
            $builder,
            'ANALYZE users(email, name)'
        );
    }

    public function test_analyze_verbose() : void
    {
        $builder = analyze()->verbose()->table('users');

        $this->assertAnalyzeQuery(
            $builder,
            'ANALYZE (VERBOSE) users'
        );
    }

    public function test_cluster_all_tables() : void
    {
        $builder = cluster();

        $this->assertClusterQuery(
            $builder,
            'CLUSTER'
        );
    }

    public function test_cluster_single_table() : void
    {
        $builder = cluster_table('users');

        $this->assertClusterQuery(
            $builder,
            'CLUSTER users'
        );
    }

    public function test_cluster_table_using_index() : void
    {
        $builder = cluster_table('users')->using('idx_users_pkey');

        $this->assertClusterQuery(
            $builder,
            'CLUSTER users USING idx_users_pkey'
        );
    }

    public function test_cluster_table_with_schema() : void
    {
        $builder = cluster_table('public.users');

        $this->assertClusterQuery(
            $builder,
            'CLUSTER public.users'
        );
    }

    public function test_cluster_verbose() : void
    {
        $builder = cluster()->verbose()->table('users');

        $this->assertClusterQuery(
            $builder,
            'CLUSTER (VERBOSE) users'
        );
    }

    public function test_comment_on_column() : void
    {
        $builder = comment(CommentTarget::COLUMN, 'users.email')->is('User email address');

        $this->assertCommentQuery(
            $builder,
            "COMMENT ON COLUMN users.email IS 'User email address'"
        );
    }

    public function test_comment_on_index() : void
    {
        $builder = comment(CommentTarget::INDEX, 'idx_users_email')->is('Email lookup index');

        $this->assertCommentQuery(
            $builder,
            "COMMENT ON INDEX idx_users_email IS 'Email lookup index'"
        );
    }

    public function test_comment_on_schema() : void
    {
        $builder = comment(CommentTarget::SCHEMA, 'public')->is('Default schema');

        $this->assertCommentQuery(
            $builder,
            "COMMENT ON SCHEMA public IS 'Default schema'"
        );
    }

    public function test_comment_on_table() : void
    {
        $builder = comment(CommentTarget::TABLE, 'users')->is('User accounts table');

        $this->assertCommentQuery(
            $builder,
            "COMMENT ON TABLE users IS 'User accounts table'"
        );
    }

    public function test_comment_remove() : void
    {
        $builder = comment(CommentTarget::TABLE, 'users')->isNull();

        $this->assertCommentQuery(
            $builder,
            'COMMENT ON TABLE users IS NULL'
        );
    }

    public function test_discard_all() : void
    {
        $builder = discard(DiscardType::ALL);

        $this->assertDiscardQuery(
            $builder,
            'DISCARD ALL'
        );
    }

    public function test_discard_plans() : void
    {
        $builder = discard(DiscardType::PLANS);

        $this->assertDiscardQuery(
            $builder,
            'DISCARD PLANS'
        );
    }

    public function test_discard_sequences() : void
    {
        $builder = discard(DiscardType::SEQUENCES);

        $this->assertDiscardQuery(
            $builder,
            'DISCARD SEQUENCES'
        );
    }

    public function test_discard_temp() : void
    {
        $builder = discard(DiscardType::TEMP);

        $this->assertDiscardQuery(
            $builder,
            'DISCARD TEMP'
        );
    }

    public function test_explain_analyze() : void
    {
        $builder = explain_analyze(select(star())->from(table('users')));

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN (ANALYZE) SELECT * FROM users'
        );
    }

    public function test_explain_format_json() : void
    {
        $builder = explain(select(star())->from(table('users')))->format(ExplainFormat::JSON);

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN (FORMAT "json") SELECT * FROM users'
        );
    }

    public function test_explain_format_xml() : void
    {
        $builder = explain(select(star())->from(table('users')))->format(ExplainFormat::XML);

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN (FORMAT xml) SELECT * FROM users'
        );
    }

    public function test_explain_format_yaml() : void
    {
        $builder = explain(select(star())->from(table('users')))->format(ExplainFormat::YAML);

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN (FORMAT yaml) SELECT * FROM users'
        );
    }

    public function test_explain_full_options() : void
    {
        $builder = explain(select(star())->from(table('users')))
            ->analyze()
            ->verbose()
            ->buffers(true)
            ->timing(true)
            ->format(ExplainFormat::JSON);

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN (ANALYZE, VERBOSE, BUFFERS 1, TIMING 1, FORMAT "json") SELECT * FROM users'
        );
    }

    public function test_explain_select() : void
    {
        $builder = explain(select(star())->from(table('users')));

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN SELECT * FROM users'
        );
    }

    public function test_explain_verbose() : void
    {
        $builder = explain(select(star())->from(table('users')))->verbose();

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN (VERBOSE) SELECT * FROM users'
        );
    }

    public function test_explain_with_buffers() : void
    {
        $builder = explain(select(star())->from(table('users')))->analyze()->buffers(true);

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN (ANALYZE, BUFFERS 1) SELECT * FROM users'
        );
    }

    public function test_explain_with_costs() : void
    {
        $builder = explain(select(star())->from(table('users')))->costs(true);

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN (COSTS 1) SELECT * FROM users'
        );
    }

    public function test_explain_with_timing() : void
    {
        $builder = explain(select(star())->from(table('users')))->analyze()->timing(true);

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN (ANALYZE, TIMING 1) SELECT * FROM users'
        );
    }

    public function test_explain_without_costs() : void
    {
        $builder = explain(select(star())->from(table('users')))->costs(false);

        $this->assertExplainQuery(
            $builder,
            'EXPLAIN (COSTS 0) SELECT * FROM users'
        );
    }

    public function test_lock_multiple_tables() : void
    {
        $builder = lock_table('users', 'orders')->exclusive();

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users, orders IN EXCLUSIVE MODE'
        );
    }

    public function test_lock_table_access_exclusive() : void
    {
        $builder = lock_table('users')->accessExclusive();

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users'
        );
    }

    public function test_lock_table_access_share() : void
    {
        $builder = lock_table('users')->accessShare();

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users IN ACCESS SHARE MODE'
        );
    }

    public function test_lock_table_default() : void
    {
        $builder = lock_table('users');

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users'
        );
    }

    public function test_lock_table_exclusive() : void
    {
        $builder = lock_table('users')->exclusive();

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users IN EXCLUSIVE MODE'
        );
    }

    public function test_lock_table_nowait() : void
    {
        $builder = lock_table('users')->exclusive()->nowait();

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users IN EXCLUSIVE MODE NOWAIT'
        );
    }

    public function test_lock_table_row_exclusive() : void
    {
        $builder = lock_table('users')->rowExclusive();

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users IN ROW EXCLUSIVE MODE'
        );
    }

    public function test_lock_table_row_share() : void
    {
        $builder = lock_table('users')->rowShare();

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users IN ROW SHARE MODE'
        );
    }

    public function test_lock_table_share() : void
    {
        $builder = lock_table('users')->share();

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users IN SHARE MODE'
        );
    }

    public function test_lock_table_share_row_exclusive() : void
    {
        $builder = lock_table('users')->shareRowExclusive();

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE'
        );
    }

    public function test_lock_table_share_update_exclusive() : void
    {
        $builder = lock_table('users')->shareUpdateExclusive();

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users IN SHARE UPDATE EXCLUSIVE MODE'
        );
    }

    public function test_lock_table_with_mode_enum() : void
    {
        $builder = lock_table('users')->inMode(LockMode::EXCLUSIVE);

        $this->assertLockQuery(
            $builder,
            'LOCK TABLE users IN EXCLUSIVE MODE'
        );
    }

    public function test_vacuum_all_tables() : void
    {
        $builder = vacuum();

        $this->assertVacuumQuery(
            $builder,
            'VACUUM'
        );
    }

    public function test_vacuum_analyze() : void
    {
        $builder = vacuum_analyze('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (ANALYZE) users'
        );
    }

    public function test_vacuum_disable_page_skipping() : void
    {
        $builder = vacuum()->disablePageSkipping()->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (DISABLE_PAGE_SKIPPING) users'
        );
    }

    public function test_vacuum_freeze() : void
    {
        $builder = vacuum()->freeze()->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (FREEZE) users'
        );
    }

    public function test_vacuum_full() : void
    {
        $builder = vacuum_full('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (FULL) users'
        );
    }

    public function test_vacuum_full_analyze_verbose() : void
    {
        $builder = vacuum()->full()->analyze()->verbose()->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (FULL, VERBOSE, ANALYZE) users'
        );
    }

    public function test_vacuum_index_cleanup_off() : void
    {
        $builder = vacuum()->indexCleanup(IndexCleanup::OFF)->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (INDEX_CLEANUP OFF) users'
        );
    }

    public function test_vacuum_index_cleanup_on() : void
    {
        $builder = vacuum()->indexCleanup(IndexCleanup::ON)->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (INDEX_CLEANUP ON) users'
        );
    }

    public function test_vacuum_multiple_tables() : void
    {
        $builder = vacuum()->tables('users', 'orders', 'products');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM users, orders, products'
        );
    }

    public function test_vacuum_parallel() : void
    {
        $builder = vacuum()->parallel(4)->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (PARALLEL 4) users'
        );
    }

    public function test_vacuum_process_main() : void
    {
        $builder = vacuum()->processMain(true)->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (PROCESS_MAIN 1) users'
        );
    }

    public function test_vacuum_process_toast() : void
    {
        $builder = vacuum()->processToast(false)->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (PROCESS_TOAST 0) users'
        );
    }

    public function test_vacuum_single_table() : void
    {
        $builder = vacuum()->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM users'
        );
    }

    public function test_vacuum_skip_locked() : void
    {
        $builder = vacuum()->skipLocked()->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (SKIP_LOCKED) users'
        );
    }

    public function test_vacuum_table_with_columns() : void
    {
        $builder = vacuum()->table('users', 'email', 'name');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM users(email, name)'
        );
    }

    public function test_vacuum_table_with_schema() : void
    {
        $builder = vacuum()->table('public.users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM public.users'
        );
    }

    public function test_vacuum_truncate_false() : void
    {
        $builder = vacuum()->truncate(false)->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (TRUNCATE 0) users'
        );
    }

    public function test_vacuum_truncate_true() : void
    {
        $builder = vacuum()->truncate(true)->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (TRUNCATE 1) users'
        );
    }

    public function test_vacuum_verbose() : void
    {
        $builder = vacuum()->verbose()->table('users');

        $this->assertVacuumQuery(
            $builder,
            'VACUUM (VERBOSE) users'
        );
    }
}
