<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Database;

use function Flow\PgQuery\DSL\{
    analyze,
    cluster,
    column,
    comment,
    create,
    explain,
    insert,
    literal,
    lock_table,
    primary_key,
    select,
    sql_type_integer,
    sql_type_serial,
    sql_type_varchar,
    star,
    table,
    vacuum
};
use Flow\PgQuery\QueryBuilder\Utility\{CommentTarget, LockMode};

final class UtilityDatabaseTest extends DatabaseTestCase
{
    private const INDEX_NAME = 'flow_postgres_utility_idx';

    private const TABLE_NAME = 'flow_postgres_utility_test';

    protected function setUp() : void
    {
        parent::setUp();

        $this->execute(
            create()->table(self::TABLE_NAME)
                ->column(column('id', sql_type_serial()))
                ->column(column('name', sql_type_varchar(100))->notNull())
                ->column(column('value', sql_type_integer())->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            insert()
                ->into(self::TABLE_NAME)
                ->columns('name', 'value')
                ->values(literal('Alice'), literal(100))
                ->values(literal('Bob'), literal(200))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->dropIndexIfExists(self::INDEX_NAME);
        $this->dropTableIfExists(self::TABLE_NAME);

        parent::tearDown();
    }

    public function test_analyze_specific_columns() : void
    {
        $result = $this->execute(
            analyze()->table(self::TABLE_NAME, 'name', 'value')
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_analyze_table() : void
    {
        $result = $this->execute(
            analyze()->tables(self::TABLE_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_analyze_verbose() : void
    {
        $result = $this->execute(
            analyze()->verbose()->tables(self::TABLE_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_cluster_table_on_index() : void
    {
        $this->execute(
            create()->index(self::INDEX_NAME)
                ->on(self::TABLE_NAME)
                ->columns('name')
                ->toSql()
        );

        $result = $this->execute(
            cluster()->table(self::TABLE_NAME)->using(self::INDEX_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_cluster_verbose() : void
    {
        $this->execute(
            create()->index(self::INDEX_NAME)
                ->on(self::TABLE_NAME)
                ->columns('name')
                ->toSql()
        );

        $this->execute(
            cluster()->table(self::TABLE_NAME)->using(self::INDEX_NAME)
                ->toSql()
        );

        $result = $this->execute(
            cluster()->verbose()->table(self::TABLE_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_comment_on_column() : void
    {
        $result = $this->execute(
            comment(CommentTarget::COLUMN, self::TABLE_NAME . '.name')
                ->is('Name of the entity')
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_comment_on_table() : void
    {
        $result = $this->execute(
            comment(CommentTarget::TABLE, self::TABLE_NAME)
                ->is('This is a test table for utility tests')
                ->toSql()
        );

        self::assertNotFalse($result);

        $row = $this->fetchOne(
            $this->execute(
                "SELECT obj_description('" . self::TABLE_NAME . "'::regclass, 'pg_class') AS comment"
            )
        );

        self::assertSame('This is a test table for utility tests', $row['comment']);
    }

    public function test_comment_remove() : void
    {
        $this->execute(
            comment(CommentTarget::TABLE, self::TABLE_NAME)
                ->is('Some comment')
                ->toSql()
        );

        $result = $this->execute(
            comment(CommentTarget::TABLE, self::TABLE_NAME)
                ->isNull()
                ->toSql()
        );

        self::assertNotFalse($result);

        $row = $this->fetchOne(
            $this->execute(
                "SELECT obj_description('" . self::TABLE_NAME . "'::regclass, 'pg_class') AS comment"
            )
        );

        self::assertNull($row['comment']);
    }

    public function test_explain_analyze() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_NAME));

        $result = $this->execute(
            explain($selectQuery)->analyze()
                ->toSql()
        );

        self::assertNotFalse($result);

        $rows = $this->fetchAll($result);
        self::assertNotEmpty($rows);
    }

    public function test_explain_select() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_NAME));

        $result = $this->execute(
            explain($selectQuery)
                ->toSql()
        );

        self::assertNotFalse($result);

        $rows = $this->fetchAll($result);
        self::assertNotEmpty($rows);
        self::assertArrayHasKey('QUERY PLAN', $rows[0]);
    }

    public function test_explain_verbose() : void
    {
        $selectQuery = select(star())->from(table(self::TABLE_NAME));

        $result = $this->execute(
            explain($selectQuery)->verbose()
                ->toSql()
        );

        self::assertNotFalse($result);

        $rows = $this->fetchAll($result);
        self::assertNotEmpty($rows);
    }

    public function test_lock_table() : void
    {
        $this->execute('BEGIN');

        $result = $this->execute(
            lock_table(self::TABLE_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);

        $this->execute('COMMIT');
    }

    public function test_lock_table_nowait() : void
    {
        $this->execute('BEGIN');

        $result = $this->execute(
            lock_table(self::TABLE_NAME)->noWait()
                ->toSql()
        );

        self::assertNotFalse($result);

        $this->execute('COMMIT');
    }

    public function test_lock_table_with_mode() : void
    {
        $this->execute('BEGIN');

        $result = $this->execute(
            lock_table(self::TABLE_NAME)->inMode(LockMode::SHARE)
                ->toSql()
        );

        self::assertNotFalse($result);

        $this->execute('COMMIT');
    }

    public function test_vacuum_analyze_table() : void
    {
        $result = $this->execute(
            vacuum()->analyze()->tables(self::TABLE_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_vacuum_full() : void
    {
        $result = $this->execute(
            vacuum()->full()->tables(self::TABLE_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
    }

    public function test_vacuum_table() : void
    {
        $result = $this->execute(
            vacuum()->tables(self::TABLE_NAME)
                ->toSql()
        );

        self::assertNotFalse($result);
    }
}
