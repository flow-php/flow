<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Utility;

use Flow\PostgreSql\QueryBuilder\Utility\CommentTarget;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardType;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFormat;
use Flow\PostgreSql\QueryBuilder\Utility\IndexCleanup;
use Flow\PostgreSql\QueryBuilder\Utility\LockMode;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\analyze;
use function Flow\PostgreSql\DSL\cluster;
use function Flow\PostgreSql\DSL\comment;
use function Flow\PostgreSql\DSL\discard;
use function Flow\PostgreSql\DSL\explain;
use function Flow\PostgreSql\DSL\lock_table;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\vacuum;

final class UtilityBuilderTest extends TestCase
{
    public function test_analyze_all_tables(): void
    {
        $builder = analyze();

        static::assertSame('ANALYZE', $builder->toSql());
    }

    public function test_analyze_multiple_tables(): void
    {
        $builder = analyze()->tables('users', 'orders', 'products');

        static::assertSame('ANALYZE users, orders, products', $builder->toSql());
    }

    public function test_analyze_single_table(): void
    {
        $builder = analyze()->table('users');

        static::assertSame('ANALYZE users', $builder->toSql());
    }

    public function test_analyze_skip_locked(): void
    {
        $builder = analyze()->skipLocked()->table('users');

        static::assertSame('ANALYZE (SKIP_LOCKED) users', $builder->toSql());
    }

    public function test_analyze_table_with_columns(): void
    {
        $builder = analyze()->table('users', 'email', 'name');

        static::assertSame('ANALYZE users(email, name)', $builder->toSql());
    }

    public function test_analyze_verbose(): void
    {
        $builder = analyze()->verbose()->table('users');

        static::assertSame('ANALYZE (VERBOSE) users', $builder->toSql());
    }

    public function test_cluster_all_tables(): void
    {
        $builder = cluster();

        static::assertSame('CLUSTER', $builder->toSql());
    }

    public function test_cluster_single_table(): void
    {
        $builder = cluster()->table('users');

        static::assertSame('CLUSTER users', $builder->toSql());
    }

    public function test_cluster_table_using_index(): void
    {
        $builder = cluster()->table('users')->using('idx_users_pkey');

        static::assertSame('CLUSTER users USING idx_users_pkey', $builder->toSql());
    }

    public function test_cluster_table_with_schema(): void
    {
        $builder = cluster()->table('public.users');

        static::assertSame('CLUSTER public.users', $builder->toSql());
    }

    public function test_cluster_verbose(): void
    {
        $builder = cluster()->verbose()->table('users');

        static::assertSame('CLUSTER (VERBOSE) users', $builder->toSql());
    }

    public function test_comment_on_column(): void
    {
        $builder = comment(CommentTarget::COLUMN, 'users.email')->is('User email address');

        static::assertSame("COMMENT ON COLUMN users.email IS 'User email address'", $builder->toSql());
    }

    public function test_comment_on_index(): void
    {
        $builder = comment(CommentTarget::INDEX, 'idx_users_email')->is('Email lookup index');

        static::assertSame("COMMENT ON INDEX idx_users_email IS 'Email lookup index'", $builder->toSql());
    }

    public function test_comment_on_schema(): void
    {
        $builder = comment(CommentTarget::SCHEMA, 'public')->is('Default schema');

        static::assertSame("COMMENT ON SCHEMA public IS 'Default schema'", $builder->toSql());
    }

    public function test_comment_on_table(): void
    {
        $builder = comment(CommentTarget::TABLE, 'users')->is('User accounts table');

        static::assertSame("COMMENT ON TABLE users IS 'User accounts table'", $builder->toSql());
    }

    public function test_comment_remove(): void
    {
        $builder = comment(CommentTarget::TABLE, 'users')->isNull();

        static::assertSame('COMMENT ON TABLE users IS NULL', $builder->toSql());
    }

    public function test_discard_all(): void
    {
        $builder = discard(DiscardType::ALL);

        static::assertSame('DISCARD ALL', $builder->toSql());
    }

    public function test_discard_plans(): void
    {
        $builder = discard(DiscardType::PLANS);

        static::assertSame('DISCARD PLANS', $builder->toSql());
    }

    public function test_discard_sequences(): void
    {
        $builder = discard(DiscardType::SEQUENCES);

        static::assertSame('DISCARD SEQUENCES', $builder->toSql());
    }

    public function test_discard_temp(): void
    {
        $builder = discard(DiscardType::TEMP);

        static::assertSame('DISCARD TEMP', $builder->toSql());
    }

    public function test_explain_analyze(): void
    {
        $builder = explain(select(star())->from(table('users')))->analyze();

        static::assertSame('EXPLAIN (ANALYZE) SELECT * FROM users', $builder->toSql());
    }

    public function test_explain_format_json(): void
    {
        $builder = explain(select(star())->from(table('users')))->format(ExplainFormat::JSON);

        static::assertSame('EXPLAIN (FORMAT "json") SELECT * FROM users', $builder->toSql());
    }

    public function test_explain_format_xml(): void
    {
        $builder = explain(select(star())->from(table('users')))->format(ExplainFormat::XML);

        static::assertSame('EXPLAIN (FORMAT xml) SELECT * FROM users', $builder->toSql());
    }

    public function test_explain_format_yaml(): void
    {
        $builder = explain(select(star())->from(table('users')))->format(ExplainFormat::YAML);

        static::assertSame('EXPLAIN (FORMAT yaml) SELECT * FROM users', $builder->toSql());
    }

    public function test_explain_full_options(): void
    {
        $builder = explain(select(star())->from(table('users')))
            ->analyze()
            ->verbose()
            ->buffers(true)
            ->timing(true)
            ->format(ExplainFormat::JSON);

        static::assertSame(
            'EXPLAIN (ANALYZE, VERBOSE, BUFFERS 1, TIMING 1, FORMAT "json") SELECT * FROM users',
            $builder->toSql(),
        );
    }

    public function test_explain_select(): void
    {
        $builder = explain(select(star())->from(table('users')));

        static::assertSame('EXPLAIN SELECT * FROM users', $builder->toSql());
    }

    public function test_explain_verbose(): void
    {
        $builder = explain(select(star())->from(table('users')))->verbose();

        static::assertSame('EXPLAIN (VERBOSE) SELECT * FROM users', $builder->toSql());
    }

    public function test_explain_with_buffers(): void
    {
        $builder = explain(select(star())->from(table('users')))->analyze()->buffers(true);

        static::assertSame('EXPLAIN (ANALYZE, BUFFERS 1) SELECT * FROM users', $builder->toSql());
    }

    public function test_explain_with_costs(): void
    {
        $builder = explain(select(star())->from(table('users')))->costs(true);

        static::assertSame('EXPLAIN (COSTS 1) SELECT * FROM users', $builder->toSql());
    }

    public function test_explain_with_timing(): void
    {
        $builder = explain(select(star())->from(table('users')))->analyze()->timing(true);

        static::assertSame('EXPLAIN (ANALYZE, TIMING 1) SELECT * FROM users', $builder->toSql());
    }

    public function test_explain_without_costs(): void
    {
        $builder = explain(select(star())->from(table('users')))->costs(false);

        static::assertSame('EXPLAIN (COSTS 0) SELECT * FROM users', $builder->toSql());
    }

    public function test_lock_multiple_tables(): void
    {
        $builder = lock_table('users', 'orders')->exclusive();

        static::assertSame('LOCK TABLE users, orders IN EXCLUSIVE MODE', $builder->toSql());
    }

    public function test_lock_table_access_exclusive(): void
    {
        $builder = lock_table('users')->accessExclusive();

        static::assertSame('LOCK TABLE users', $builder->toSql());
    }

    public function test_lock_table_access_share(): void
    {
        $builder = lock_table('users')->accessShare();

        static::assertSame('LOCK TABLE users IN ACCESS SHARE MODE', $builder->toSql());
    }

    public function test_lock_table_default(): void
    {
        $builder = lock_table('users');

        static::assertSame('LOCK TABLE users', $builder->toSql());
    }

    public function test_lock_table_exclusive(): void
    {
        $builder = lock_table('users')->exclusive();

        static::assertSame('LOCK TABLE users IN EXCLUSIVE MODE', $builder->toSql());
    }

    public function test_lock_table_nowait(): void
    {
        $builder = lock_table('users')->exclusive()->nowait();

        static::assertSame('LOCK TABLE users IN EXCLUSIVE MODE NOWAIT', $builder->toSql());
    }

    public function test_lock_table_row_exclusive(): void
    {
        $builder = lock_table('users')->rowExclusive();

        static::assertSame('LOCK TABLE users IN ROW EXCLUSIVE MODE', $builder->toSql());
    }

    public function test_lock_table_row_share(): void
    {
        $builder = lock_table('users')->rowShare();

        static::assertSame('LOCK TABLE users IN ROW SHARE MODE', $builder->toSql());
    }

    public function test_lock_table_share(): void
    {
        $builder = lock_table('users')->share();

        static::assertSame('LOCK TABLE users IN SHARE MODE', $builder->toSql());
    }

    public function test_lock_table_share_row_exclusive(): void
    {
        $builder = lock_table('users')->shareRowExclusive();

        static::assertSame('LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE', $builder->toSql());
    }

    public function test_lock_table_share_update_exclusive(): void
    {
        $builder = lock_table('users')->shareUpdateExclusive();

        static::assertSame('LOCK TABLE users IN SHARE UPDATE EXCLUSIVE MODE', $builder->toSql());
    }

    public function test_lock_table_with_mode_enum(): void
    {
        $builder = lock_table('users')->inMode(LockMode::EXCLUSIVE);

        static::assertSame('LOCK TABLE users IN EXCLUSIVE MODE', $builder->toSql());
    }

    public function test_vacuum_all_tables(): void
    {
        $builder = vacuum();

        static::assertSame('VACUUM', $builder->toSql());
    }

    public function test_vacuum_analyze(): void
    {
        $builder = vacuum()->analyze()->tables('users');

        static::assertSame('VACUUM (ANALYZE) users', $builder->toSql());
    }

    public function test_vacuum_disable_page_skipping(): void
    {
        $builder = vacuum()->disablePageSkipping()->table('users');

        static::assertSame('VACUUM (DISABLE_PAGE_SKIPPING) users', $builder->toSql());
    }

    public function test_vacuum_freeze(): void
    {
        $builder = vacuum()->freeze()->table('users');

        static::assertSame('VACUUM (FREEZE) users', $builder->toSql());
    }

    public function test_vacuum_full(): void
    {
        $builder = vacuum()->full()->tables('users');

        static::assertSame('VACUUM (FULL) users', $builder->toSql());
    }

    public function test_vacuum_full_analyze_verbose(): void
    {
        $builder = vacuum()->full()->analyze()->verbose()->table('users');

        static::assertSame('VACUUM (FULL, VERBOSE, ANALYZE) users', $builder->toSql());
    }

    public function test_vacuum_index_cleanup_off(): void
    {
        $builder = vacuum()->indexCleanup(IndexCleanup::OFF)->table('users');

        static::assertSame('VACUUM (INDEX_CLEANUP OFF) users', $builder->toSql());
    }

    public function test_vacuum_index_cleanup_on(): void
    {
        $builder = vacuum()->indexCleanup(IndexCleanup::ON)->table('users');

        static::assertSame('VACUUM (INDEX_CLEANUP ON) users', $builder->toSql());
    }

    public function test_vacuum_multiple_tables(): void
    {
        $builder = vacuum()->tables('users', 'orders', 'products');

        static::assertSame('VACUUM users, orders, products', $builder->toSql());
    }

    public function test_vacuum_parallel(): void
    {
        $builder = vacuum()->parallel(4)->table('users');

        static::assertSame('VACUUM (PARALLEL 4) users', $builder->toSql());
    }

    public function test_vacuum_process_main(): void
    {
        $builder = vacuum()->processMain(true)->table('users');

        static::assertSame('VACUUM (PROCESS_MAIN 1) users', $builder->toSql());
    }

    public function test_vacuum_process_toast(): void
    {
        $builder = vacuum()->processToast(false)->table('users');

        static::assertSame('VACUUM (PROCESS_TOAST 0) users', $builder->toSql());
    }

    public function test_vacuum_single_table(): void
    {
        $builder = vacuum()->table('users');

        static::assertSame('VACUUM users', $builder->toSql());
    }

    public function test_vacuum_skip_locked(): void
    {
        $builder = vacuum()->skipLocked()->table('users');

        static::assertSame('VACUUM (SKIP_LOCKED) users', $builder->toSql());
    }

    public function test_vacuum_table_with_columns(): void
    {
        $builder = vacuum()->table('users', 'email', 'name');

        static::assertSame('VACUUM users(email, name)', $builder->toSql());
    }

    public function test_vacuum_table_with_schema(): void
    {
        $builder = vacuum()->table('public.users');

        static::assertSame('VACUUM public.users', $builder->toSql());
    }

    public function test_vacuum_truncate_false(): void
    {
        $builder = vacuum()->truncate(false)->table('users');

        static::assertSame('VACUUM (TRUNCATE 0) users', $builder->toSql());
    }

    public function test_vacuum_truncate_true(): void
    {
        $builder = vacuum()->truncate(true)->table('users');

        static::assertSame('VACUUM (TRUNCATE 1) users', $builder->toSql());
    }

    public function test_vacuum_verbose(): void
    {
        $builder = vacuum()->verbose()->table('users');

        static::assertSame('VACUUM (VERBOSE) users', $builder->toSql());
    }
}
