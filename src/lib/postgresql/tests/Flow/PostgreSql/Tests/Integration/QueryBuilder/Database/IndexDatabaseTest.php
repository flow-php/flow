<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    agg_count,
    col,
    column,
    create,
    drop,
    eq,
    index_col,
    index_method_btree,
    index_method_hash,
    insert,
    literal,
    select,
    sql_type_integer,
    sql_type_serial,
    sql_type_varchar,
    star,
    table
};

final class IndexDatabaseTest extends DatabaseTestCase
{
    private const INDEX_COMPOSITE = 'flow_postgres_idx_composite';

    private const INDEX_EMAIL = 'flow_postgres_idx_email';

    private const INDEX_NAME = 'flow_postgres_idx_name';

    private const TABLE_INDEXED = 'flow_postgres_indexed_table';

    protected function setUp() : void
    {
        parent::setUp();

        $query = create()->table(self::TABLE_INDEXED)
            ->column(column('id', sql_type_serial()))
            ->column(column('name', sql_type_varchar(100)))
            ->column(column('email', sql_type_varchar(255)))
            ->column(column('age', sql_type_integer()));

        $this->execute($query->toSql());

        $this->execute(
            insert()
                ->into(self::TABLE_INDEXED)
                ->columns('name', 'email', 'age')
                ->values(literal('Test'), literal('test@example.com'), literal(30))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->dropIndexIfExists(self::INDEX_NAME);
        $this->dropIndexIfExists(self::INDEX_EMAIL);
        $this->dropIndexIfExists(self::INDEX_COMPOSITE);
        $this->dropTableIfExists(self::TABLE_INDEXED);

        parent::tearDown();
    }

    public function test_create_composite_index() : void
    {
        $query = create()->index(self::INDEX_COMPOSITE)
            ->on(self::TABLE_INDEXED)
            ->columns(index_col('name'), index_col('email'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
    }

    public function test_create_index() : void
    {
        $query = create()->index(self::INDEX_NAME)
            ->on(self::TABLE_INDEXED)
            ->columns(index_col('name'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $check = $this->execute(
            select(agg_count(star())->as('cnt'))
                ->from(table('pg_indexes'))
                ->where(
                    eq(col('tablename'), literal(self::TABLE_INDEXED))
                )
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertGreaterThanOrEqual(1, (int) $row['cnt']);
    }

    public function test_create_index_if_not_exists() : void
    {
        $query = create()->index(self::INDEX_NAME)
            ->ifNotExists()
            ->on(self::TABLE_INDEXED)
            ->columns(index_col('name'));

        $this->execute($query->toSql());
        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
    }

    public function test_create_index_with_btree_method() : void
    {
        $query = create()->index(self::INDEX_NAME)
            ->on(self::TABLE_INDEXED)
            ->using(index_method_btree())
            ->columns(index_col('name'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
    }

    public function test_create_index_with_hash_method() : void
    {
        $query = create()->index(self::INDEX_NAME)
            ->on(self::TABLE_INDEXED)
            ->using(index_method_hash())
            ->columns(index_col('name'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
    }

    public function test_create_unique_index() : void
    {
        $query = create()->index(self::INDEX_EMAIL)
            ->unique()
            ->on(self::TABLE_INDEXED)
            ->columns(index_col('email'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);

        $duplicateResult = @$this->execute(
            insert()
                ->into(self::TABLE_INDEXED)
                ->columns('name', 'email', 'age')
                ->values(literal('Test2'), literal('test@example.com'), literal(25))
                ->toSql()
        );
        self::assertFalse($duplicateResult);
    }

    public function test_drop_index() : void
    {
        $createQuery = create()->index(self::INDEX_NAME)
            ->on(self::TABLE_INDEXED)
            ->columns(index_col('name'));
        $this->execute($createQuery->toSql());

        $dropQuery = drop()->index(self::INDEX_NAME);

        $result = $this->execute($dropQuery->toSql());

        self::assertNotFalse($result);
    }

    public function test_drop_index_if_exists() : void
    {
        $dropQuery = drop()->index(self::INDEX_NAME)->ifExists();

        $result = $this->execute($dropQuery->toSql());

        self::assertNotFalse($result);
    }
}
