<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Index;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\index_col;
use function Flow\PostgreSql\DSL\index_method_btree;
use function Flow\PostgreSql\DSL\index_method_gin;
use function Flow\PostgreSql\DSL\index_method_hash;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\reindex_database;
use function Flow\PostgreSql\DSL\reindex_index;
use function Flow\PostgreSql\DSL\reindex_schema;
use function Flow\PostgreSql\DSL\reindex_table;

final class IndexBuilderTest extends TestCase
{
    public function test_alter_index_rename(): void
    {
        $builder = alter()->index('idx_old')->renameTo('idx_new');

        static::assertSame('ALTER INDEX idx_old RENAME TO idx_new', $builder->toSql());
    }

    public function test_alter_index_rename_if_exists(): void
    {
        $builder = alter()->index('idx_old')->ifExists()->renameTo('idx_new');

        static::assertSame('ALTER INDEX IF EXISTS idx_old RENAME TO idx_new', $builder->toSql());
    }

    public function test_alter_index_rename_with_schema(): void
    {
        $builder = alter()->index('idx_old', 'public')->renameTo('idx_new');

        static::assertSame('ALTER INDEX public.idx_old RENAME TO idx_new', $builder->toSql());
    }

    public function test_alter_index_set_tablespace(): void
    {
        $builder = alter()->index('idx_users_email')->setTablespace('fast_storage');

        static::assertSame('ALTER INDEX idx_users_email SET TABLESPACE fast_storage', $builder->toSql());
    }

    public function test_alter_index_set_tablespace_if_exists(): void
    {
        $builder = alter()->index('idx_users_email')->ifExists()->setTablespace('fast_storage');

        static::assertSame('ALTER INDEX IF EXISTS idx_users_email SET TABLESPACE fast_storage', $builder->toSql());
    }

    public function test_create_index_concurrently(): void
    {
        $builder = create()->index('idx_users_email')->concurrently()->on('users')->columns('email');

        static::assertSame('CREATE INDEX CONCURRENTLY idx_users_email ON users (email)', $builder->toSql());
    }

    public function test_create_index_if_not_exists(): void
    {
        $builder = create()->index('idx_users_email')->ifNotExists()->on('users')->columns('email');

        static::assertSame('CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)', $builder->toSql());
    }

    public function test_create_index_simple(): void
    {
        $builder = create()->index('idx_users_email')->on('users')->columns('email');

        static::assertSame('CREATE INDEX idx_users_email ON users (email)', $builder->toSql());
    }

    public function test_create_index_unique(): void
    {
        $builder = create()->index('idx_users_email')->unique()->on('users')->columns('email');

        static::assertSame('CREATE UNIQUE INDEX idx_users_email ON users (email)', $builder->toSql());
    }

    public function test_create_index_unique_concurrently_if_not_exists(): void
    {
        $builder = create()
            ->index('idx_users_email')
            ->unique()
            ->concurrently()
            ->ifNotExists()
            ->on('users')
            ->columns('email');

        static::assertSame(
            'CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users (email)',
            $builder->toSql(),
        );
    }

    public function test_create_index_with_btree_method(): void
    {
        $builder = create()->index('idx_users_email')->on('users')->using(index_method_btree())->columns('email');

        static::assertSame('CREATE INDEX idx_users_email ON users USING btree (email)', $builder->toSql());
    }

    public function test_create_index_with_desc_order(): void
    {
        $builder = create()->index('idx_users_created_at')->on('users')->columns(index_col('created_at')->desc());

        static::assertSame('CREATE INDEX idx_users_created_at ON users (created_at DESC)', $builder->toSql());
    }

    public function test_create_index_with_gin_method(): void
    {
        $builder = create()
            ->index('idx_documents_content')
            ->on('documents')
            ->using(index_method_gin())
            ->columns('content');

        static::assertSame('CREATE INDEX idx_documents_content ON documents USING gin (content)', $builder->toSql());
    }

    public function test_create_index_with_hash_method(): void
    {
        $builder = create()->index('idx_users_email')->on('users')->using(index_method_hash())->columns('email');

        static::assertSame('CREATE INDEX idx_users_email ON users USING hash (email)', $builder->toSql());
    }

    public function test_create_index_with_include(): void
    {
        $builder = create()->index('idx_users_email')->on('users')->columns('email')->include('name', 'created_at');

        static::assertSame(
            'CREATE INDEX idx_users_email ON users (email) INCLUDE (name, created_at)',
            $builder->toSql(),
        );
    }

    public function test_create_index_with_multiple_columns(): void
    {
        $builder = create()->index('idx_users_name_email')->on('users')->columns('name', 'email');

        static::assertSame('CREATE INDEX idx_users_name_email ON users (name, email)', $builder->toSql());
    }

    public function test_create_index_with_nulls_first(): void
    {
        $builder = create()
            ->index('idx_users_created_at')
            ->on('users')
            ->columns(index_col('created_at')->desc()->nullsFirst());

        static::assertSame(
            'CREATE INDEX idx_users_created_at ON users (created_at DESC NULLS FIRST)',
            $builder->toSql(),
        );
    }

    public function test_create_index_with_nulls_last(): void
    {
        $builder = create()->index('idx_users_created_at')->on('users')->columns(index_col('created_at')->nullsLast());

        static::assertSame('CREATE INDEX idx_users_created_at ON users (created_at NULLS LAST)', $builder->toSql());
    }

    public function test_create_index_with_nulls_not_distinct(): void
    {
        $builder = create()->index('idx_users_email')->unique()->on('users')->columns('email')->nullsNotDistinct();

        static::assertSame(
            'CREATE UNIQUE INDEX idx_users_email ON users (email) NULLS NOT DISTINCT',
            $builder->toSql(),
        );
    }

    public function test_create_index_with_on_only(): void
    {
        $builder = create()->index('idx_users_email')->onOnly('users')->columns('email');

        static::assertSame('CREATE INDEX idx_users_email ON ONLY users (email)', $builder->toSql());
    }

    public function test_create_index_with_schema(): void
    {
        $builder = create()->index('idx_users_email')->on('users', 'public')->columns('email');

        static::assertSame('CREATE INDEX idx_users_email ON public.users (email)', $builder->toSql());
    }

    public function test_create_index_with_tablespace(): void
    {
        $builder = create()->index('idx_users_email')->on('users')->columns('email')->tablespace('fast_storage');

        static::assertSame('CREATE INDEX idx_users_email ON users (email) TABLESPACE fast_storage', $builder->toSql());
    }

    public function test_create_partial_index(): void
    {
        $builder = create()
            ->index('idx_users_active_email')
            ->on('users')
            ->columns('email')
            ->where(eq(col('active'), literal(true)));

        static::assertSame(
            'CREATE INDEX idx_users_active_email ON users (email) WHERE active = true',
            $builder->toSql(),
        );
    }

    public function test_drop_index_cascade(): void
    {
        $builder = drop()->index('idx_users_email')->cascade();

        static::assertSame('DROP INDEX idx_users_email CASCADE', $builder->toSql());
    }

    public function test_drop_index_concurrently(): void
    {
        $builder = drop()->index('idx_users_email')->concurrently();

        static::assertSame('DROP INDEX CONCURRENTLY idx_users_email', $builder->toSql());
    }

    public function test_drop_index_if_exists(): void
    {
        $builder = drop()->index('idx_users_email')->ifExists();

        static::assertSame('DROP INDEX IF EXISTS idx_users_email', $builder->toSql());
    }

    public function test_drop_index_if_exists_cascade(): void
    {
        $builder = drop()->index('idx_users_email')->ifExists()->cascade();

        static::assertSame('DROP INDEX IF EXISTS idx_users_email CASCADE', $builder->toSql());
    }

    public function test_drop_index_multiple(): void
    {
        $builder = drop()->index('idx_users_email', 'idx_users_name', 'idx_orders_date');

        static::assertSame('DROP INDEX idx_users_email, idx_users_name, idx_orders_date', $builder->toSql());
    }

    public function test_drop_index_restrict(): void
    {
        $builder = drop()->index('idx_users_email')->restrict();

        static::assertSame('DROP INDEX idx_users_email', $builder->toSql());
    }

    public function test_drop_index_simple(): void
    {
        $builder = drop()->index('idx_users_email');

        static::assertSame('DROP INDEX idx_users_email', $builder->toSql());
    }

    public function test_drop_index_with_schema(): void
    {
        $builder = drop()->index('public.idx_users_email');

        static::assertSame('DROP INDEX public.idx_users_email', $builder->toSql());
    }

    public function test_reindex_database(): void
    {
        $builder = reindex_database('mydb');

        static::assertSame('REINDEX DATABASE mydb', $builder->toSql());
    }

    public function test_reindex_index(): void
    {
        $builder = reindex_index('idx_users_email');

        static::assertSame('REINDEX INDEX idx_users_email', $builder->toSql());
    }

    public function test_reindex_index_concurrently(): void
    {
        $builder = reindex_index('idx_users_email')->concurrently();

        static::assertSame('REINDEX (CONCURRENTLY) INDEX idx_users_email', $builder->toSql());
    }

    public function test_reindex_index_with_tablespace(): void
    {
        $builder = reindex_index('idx_users_email')->tablespace('fast_storage');

        static::assertSame('REINDEX (TABLESPACE fast_storage) INDEX idx_users_email', $builder->toSql());
    }

    public function test_reindex_schema(): void
    {
        $builder = reindex_schema('public');

        static::assertSame('REINDEX SCHEMA public', $builder->toSql());
    }

    public function test_reindex_table(): void
    {
        $builder = reindex_table('users');

        static::assertSame('REINDEX TABLE users', $builder->toSql());
    }

    public function test_reindex_table_concurrently(): void
    {
        $builder = reindex_table('users')->concurrently();

        static::assertSame('REINDEX (CONCURRENTLY) TABLE users', $builder->toSql());
    }

    public function test_reindex_table_verbose(): void
    {
        $builder = reindex_table('users')->verbose();

        static::assertSame('REINDEX (VERBOSE) TABLE users', $builder->toSql());
    }

    public function test_reindex_table_with_schema(): void
    {
        $builder = reindex_table('public.users');

        static::assertSame('REINDEX TABLE public.users', $builder->toSql());
    }
}
