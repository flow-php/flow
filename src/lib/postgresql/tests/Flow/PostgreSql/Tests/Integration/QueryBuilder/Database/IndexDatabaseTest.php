<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    agg_count,
    col,
    column,
    column_type_integer,
    column_type_serial,
    column_type_varchar,
    create,
    drop,
    eq,
    index_col,
    index_method_btree,
    index_method_hash,
    insert,
    literal,
    select,
    star,
    table
};
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class IndexDatabaseTest extends PostgreSqlTestCase
{
    private const INDEX_COMPOSITE = 'flow_postgres_idx_composite';

    private const INDEX_EMAIL = 'flow_postgres_idx_email';

    private const INDEX_NAME = 'flow_postgres_idx_name';

    private const TABLE_INDEXED = 'flow_postgres_indexed_table';

    protected function setUp() : void
    {
        parent::setUp();

        $query = create()->table(self::TABLE_INDEXED)
            ->column(column('id', column_type_serial()))
            ->column(column('name', column_type_varchar(100)))
            ->column(column('email', column_type_varchar(255)))
            ->column(column('age', column_type_integer()));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_INDEXED)
                ->columns('name', 'email', 'age')
                ->values(literal('Test'), literal('test@example.com'), literal(30))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->pgsqlContext()->dropIndexIfExists(self::INDEX_NAME);
        $this->pgsqlContext()->dropIndexIfExists(self::INDEX_EMAIL);
        $this->pgsqlContext()->dropIndexIfExists(self::INDEX_COMPOSITE);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_INDEXED);

        parent::tearDown();
    }

    public function test_create_composite_index() : void
    {
        $query = create()->index(self::INDEX_COMPOSITE)
            ->on(self::TABLE_INDEXED)
            ->columns(index_col('name'), index_col('email'));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count(star())->as('cnt'))
                ->from(table('pg_indexes'))
                ->where(eq(col('indexname'), literal(self::INDEX_COMPOSITE)))
                ->toSql()
        );
        self::assertSame(1, (int) $row['cnt']);
    }

    public function test_create_index() : void
    {
        $query = create()->index(self::INDEX_NAME)
            ->on(self::TABLE_INDEXED)
            ->columns(index_col('name'));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count(star())->as('cnt'))
                ->from(table('pg_indexes'))
                ->where(
                    eq(col('tablename'), literal(self::TABLE_INDEXED))
                )
                ->toSql()
        );
        self::assertGreaterThanOrEqual(1, (int) $row['cnt']);
    }

    public function test_create_index_if_not_exists() : void
    {
        $query = create()->index(self::INDEX_NAME)
            ->ifNotExists()
            ->on(self::TABLE_INDEXED)
            ->columns(index_col('name'));

        $this->pgsqlContext()->client()->execute($query->toSql());
        $this->pgsqlContext()->client()->execute($query->toSql());

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count(star())->as('cnt'))
                ->from(table('pg_indexes'))
                ->where(eq(col('indexname'), literal(self::INDEX_NAME)))
                ->toSql()
        );
        self::assertSame(1, (int) $row['cnt']);
    }

    public function test_create_index_with_btree_method() : void
    {
        $query = create()->index(self::INDEX_NAME)
            ->on(self::TABLE_INDEXED)
            ->using(index_method_btree())
            ->columns(index_col('name'));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count(star())->as('cnt'))
                ->from(table('pg_indexes'))
                ->where(eq(col('indexname'), literal(self::INDEX_NAME)))
                ->toSql()
        );
        self::assertSame(1, (int) $row['cnt']);
    }

    public function test_create_index_with_hash_method() : void
    {
        $query = create()->index(self::INDEX_NAME)
            ->on(self::TABLE_INDEXED)
            ->using(index_method_hash())
            ->columns(index_col('name'));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count(star())->as('cnt'))
                ->from(table('pg_indexes'))
                ->where(eq(col('indexname'), literal(self::INDEX_NAME)))
                ->toSql()
        );
        self::assertSame(1, (int) $row['cnt']);
    }

    public function test_create_unique_index() : void
    {
        $query = create()->index(self::INDEX_EMAIL)
            ->unique()
            ->on(self::TABLE_INDEXED)
            ->columns(index_col('email'));

        $this->pgsqlContext()->client()->execute($query->toSql());

        $this->expectException(QueryException::class);
        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_INDEXED)
                ->columns('name', 'email', 'age')
                ->values(literal('Test2'), literal('test@example.com'), literal(25))
                ->toSql()
        );
    }

    public function test_drop_index() : void
    {
        $createQuery = create()->index(self::INDEX_NAME)
            ->on(self::TABLE_INDEXED)
            ->columns(index_col('name'));
        $this->pgsqlContext()->client()->execute($createQuery->toSql());

        $dropQuery = drop()->index(self::INDEX_NAME);
        $this->pgsqlContext()->client()->execute($dropQuery->toSql());

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count(star())->as('cnt'))
                ->from(table('pg_indexes'))
                ->where(eq(col('indexname'), literal(self::INDEX_NAME)))
                ->toSql()
        );
        self::assertSame(0, (int) $row['cnt']);
    }

    public function test_drop_index_if_exists() : void
    {
        $dropQuery = drop()->index(self::INDEX_NAME)->ifExists();
        $this->pgsqlContext()->client()->execute($dropQuery->toSql());

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count(star())->as('cnt'))
                ->from(table('pg_indexes'))
                ->where(eq(col('indexname'), literal(self::INDEX_NAME)))
                ->toSql()
        );
        self::assertSame(0, (int) $row['cnt']);
    }
}
