<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder;

use function Flow\PostgreSql\DSL\{alter,
    col,
    create,
    drop,
    eq,
    index_col,
    index_method_btree,
    index_method_gin,
    index_method_hash,
    literal,
    reindex_database,
    reindex_index,
    reindex_schema,
    reindex_table};

final class IndexBuilderTest extends PGQueryTestCase
{
    public function test_alter_index_rename() : void
    {
        $builder = alter()->index('idx_old')
            ->renameTo('idx_new');

        $this->assertAlterIndexRenameQuery(
            $builder,
            'ALTER INDEX idx_old RENAME TO idx_new'
        );
    }

    public function test_alter_index_rename_if_exists() : void
    {
        $builder = alter()->index('idx_old')
            ->ifExists()
            ->renameTo('idx_new');

        $this->assertAlterIndexRenameQuery(
            $builder,
            'ALTER INDEX IF EXISTS idx_old RENAME TO idx_new'
        );
    }

    public function test_alter_index_rename_with_schema() : void
    {
        $builder = alter()->index('idx_old', 'public')
            ->renameTo('idx_new');

        $this->assertAlterIndexRenameQuery(
            $builder,
            'ALTER INDEX public.idx_old RENAME TO idx_new'
        );
    }

    public function test_alter_index_set_tablespace() : void
    {
        $builder = alter()->index('idx_users_email')
            ->setTablespace('fast_storage');

        $this->assertAlterIndexTablespaceQuery(
            $builder,
            'ALTER INDEX idx_users_email SET TABLESPACE fast_storage'
        );
    }

    public function test_alter_index_set_tablespace_if_exists() : void
    {
        $builder = alter()->index('idx_users_email')
            ->ifExists()
            ->setTablespace('fast_storage');

        $this->assertAlterIndexTablespaceQuery(
            $builder,
            'ALTER INDEX IF EXISTS idx_users_email SET TABLESPACE fast_storage'
        );
    }

    public function test_create_index_concurrently() : void
    {
        $builder = create()->index('idx_users_email')
            ->concurrently()
            ->on('users')
            ->columns('email');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX CONCURRENTLY idx_users_email ON users (email)'
        );
    }

    public function test_create_index_if_not_exists() : void
    {
        $builder = create()->index('idx_users_email')
            ->ifNotExists()
            ->on('users')
            ->columns('email');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)'
        );
    }

    public function test_create_index_simple() : void
    {
        $builder = create()->index('idx_users_email')
            ->on('users')
            ->columns('email');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_email ON users (email)'
        );
    }

    public function test_create_index_unique() : void
    {
        $builder = create()->index('idx_users_email')
            ->unique()
            ->on('users')
            ->columns('email');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE UNIQUE INDEX idx_users_email ON users (email)'
        );
    }

    public function test_create_index_unique_concurrently_if_not_exists() : void
    {
        $builder = create()->index('idx_users_email')
            ->unique()
            ->concurrently()
            ->ifNotExists()
            ->on('users')
            ->columns('email');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users (email)'
        );
    }

    public function test_create_index_with_btree_method() : void
    {
        $builder = create()->index('idx_users_email')
            ->on('users')
            ->using(index_method_btree())
            ->columns('email');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_email ON users USING btree (email)'
        );
    }

    public function test_create_index_with_desc_order() : void
    {
        $builder = create()->index('idx_users_created_at')
            ->on('users')
            ->columns(index_col('created_at')->desc());

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_created_at ON users (created_at DESC)'
        );
    }

    public function test_create_index_with_gin_method() : void
    {
        $builder = create()->index('idx_documents_content')
            ->on('documents')
            ->using(index_method_gin())
            ->columns('content');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_documents_content ON documents USING gin (content)'
        );
    }

    public function test_create_index_with_hash_method() : void
    {
        $builder = create()->index('idx_users_email')
            ->on('users')
            ->using(index_method_hash())
            ->columns('email');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_email ON users USING hash (email)'
        );
    }

    public function test_create_index_with_include() : void
    {
        $builder = create()->index('idx_users_email')
            ->on('users')
            ->columns('email')
            ->include('name', 'created_at');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_email ON users (email) INCLUDE (name, created_at)'
        );
    }

    public function test_create_index_with_multiple_columns() : void
    {
        $builder = create()->index('idx_users_name_email')
            ->on('users')
            ->columns('name', 'email');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_name_email ON users (name, email)'
        );
    }

    public function test_create_index_with_nulls_first() : void
    {
        $builder = create()->index('idx_users_created_at')
            ->on('users')
            ->columns(index_col('created_at')->desc()->nullsFirst());

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_created_at ON users (created_at DESC NULLS FIRST)'
        );
    }

    public function test_create_index_with_nulls_last() : void
    {
        $builder = create()->index('idx_users_created_at')
            ->on('users')
            ->columns(index_col('created_at')->nullsLast());

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_created_at ON users (created_at NULLS LAST)'
        );
    }

    public function test_create_index_with_nulls_not_distinct() : void
    {
        $builder = create()->index('idx_users_email')
            ->unique()
            ->on('users')
            ->columns('email')
            ->nullsNotDistinct();

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE UNIQUE INDEX idx_users_email ON users (email) NULLS NOT DISTINCT'
        );
    }

    public function test_create_index_with_on_only() : void
    {
        $builder = create()->index('idx_users_email')
            ->onOnly('users')
            ->columns('email');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_email ON ONLY users (email)'
        );
    }

    public function test_create_index_with_schema() : void
    {
        $builder = create()->index('idx_users_email')
            ->on('users', 'public')
            ->columns('email');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_email ON public.users (email)'
        );
    }

    public function test_create_index_with_tablespace() : void
    {
        $builder = create()->index('idx_users_email')
            ->on('users')
            ->columns('email')
            ->tablespace('fast_storage');

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_email ON users (email) TABLESPACE fast_storage'
        );
    }

    public function test_create_partial_index() : void
    {
        $builder = create()->index('idx_users_active_email')
            ->on('users')
            ->columns('email')
            ->where(eq(col('active'), literal(true)));

        $this->assertCreateIndexQuery(
            $builder,
            'CREATE INDEX idx_users_active_email ON users (email) WHERE active = true'
        );
    }

    public function test_drop_index_cascade() : void
    {
        $builder = drop()->index('idx_users_email')
            ->cascade();

        $this->assertDropIndexQuery(
            $builder,
            'DROP INDEX idx_users_email CASCADE'
        );
    }

    public function test_drop_index_concurrently() : void
    {
        $builder = drop()->index('idx_users_email')
            ->concurrently();

        $this->assertDropIndexQuery(
            $builder,
            'DROP INDEX CONCURRENTLY idx_users_email'
        );
    }

    public function test_drop_index_if_exists() : void
    {
        $builder = drop()->index('idx_users_email')
            ->ifExists();

        $this->assertDropIndexQuery(
            $builder,
            'DROP INDEX IF EXISTS idx_users_email'
        );
    }

    public function test_drop_index_if_exists_cascade() : void
    {
        $builder = drop()->index('idx_users_email')
            ->ifExists()
            ->cascade();

        $this->assertDropIndexQuery(
            $builder,
            'DROP INDEX IF EXISTS idx_users_email CASCADE'
        );
    }

    public function test_drop_index_multiple() : void
    {
        $builder = drop()->index('idx_users_email', 'idx_users_name', 'idx_orders_date');

        $this->assertDropIndexQuery(
            $builder,
            'DROP INDEX idx_users_email, idx_users_name, idx_orders_date'
        );
    }

    public function test_drop_index_restrict() : void
    {
        $builder = drop()->index('idx_users_email')
            ->restrict();

        $this->assertDropIndexQuery(
            $builder,
            'DROP INDEX idx_users_email'
        );
    }

    public function test_drop_index_simple() : void
    {
        $builder = drop()->index('idx_users_email');

        $this->assertDropIndexQuery(
            $builder,
            'DROP INDEX idx_users_email'
        );
    }

    public function test_drop_index_with_schema() : void
    {
        $builder = drop()->index('public.idx_users_email');

        $this->assertDropIndexQuery(
            $builder,
            'DROP INDEX public.idx_users_email'
        );
    }

    public function test_reindex_database() : void
    {
        $builder = reindex_database('mydb');

        $this->assertReindexQuery(
            $builder,
            'REINDEX DATABASE mydb'
        );
    }

    public function test_reindex_index() : void
    {
        $builder = reindex_index('idx_users_email');

        $this->assertReindexQuery(
            $builder,
            'REINDEX INDEX idx_users_email'
        );
    }

    public function test_reindex_index_concurrently() : void
    {
        $builder = reindex_index('idx_users_email')
            ->concurrently();

        $this->assertReindexQuery(
            $builder,
            'REINDEX (CONCURRENTLY) INDEX idx_users_email'
        );
    }

    public function test_reindex_index_with_tablespace() : void
    {
        $builder = reindex_index('idx_users_email')
            ->tablespace('fast_storage');

        $this->assertReindexQuery(
            $builder,
            'REINDEX (TABLESPACE fast_storage) INDEX idx_users_email'
        );
    }

    public function test_reindex_schema() : void
    {
        $builder = reindex_schema('public');

        $this->assertReindexQuery(
            $builder,
            'REINDEX SCHEMA public'
        );
    }

    public function test_reindex_table() : void
    {
        $builder = reindex_table('users');

        $this->assertReindexQuery(
            $builder,
            'REINDEX TABLE users'
        );
    }

    public function test_reindex_table_concurrently() : void
    {
        $builder = reindex_table('users')
            ->concurrently();

        $this->assertReindexQuery(
            $builder,
            'REINDEX (CONCURRENTLY) TABLE users'
        );
    }

    public function test_reindex_table_verbose() : void
    {
        $builder = reindex_table('users')
            ->verbose();

        $this->assertReindexQuery(
            $builder,
            'REINDEX (VERBOSE) TABLE users'
        );
    }

    public function test_reindex_table_with_schema() : void
    {
        $builder = reindex_table('public.users');

        $this->assertReindexQuery(
            $builder,
            'REINDEX TABLE public.users'
        );
    }
}
