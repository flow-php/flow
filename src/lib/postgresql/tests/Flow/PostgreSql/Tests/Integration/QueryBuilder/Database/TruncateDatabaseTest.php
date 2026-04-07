<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    col,
    column,
    column_type_serial,
    column_type_varchar,
    create,
    desc,
    insert,
    literal,
    primary_key,
    select,
    star,
    table,
    truncate_table
};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class TruncateDatabaseTest extends PostgreSqlTestCase
{
    private const SCHEMA_NAME = 'flow_postgres_test_truncate_schema';

    private const SCHEMA_TABLE = 'flow_postgres_schema_truncate';

    private const TABLE_ONE = 'flow_postgres_truncate_one';

    private const TABLE_TWO = 'flow_postgres_truncate_two';

    protected function setUp() : void
    {
        parent::setUp();

        $this->pgsqlContext()->client()->execute(
            create()->table(self::TABLE_ONE)
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_varchar(100))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->table(self::TABLE_TWO)
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_varchar(100))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_ONE)
                ->columns('name')
                ->values(literal('Alice'))
                ->values(literal('Bob'))
                ->values(literal('Charlie'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_TWO)
                ->columns('name')
                ->values(literal('Dave'))
                ->values(literal('Eve'))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_TWO);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_ONE);
        $this->pgsqlContext()->client()->execute('DROP SCHEMA IF EXISTS ' . self::SCHEMA_NAME . ' CASCADE');

        parent::tearDown();
    }

    public function test_truncate_cascade() : void
    {
        $this->pgsqlContext()->client()->execute(
            truncate_table(self::TABLE_ONE)->cascade()->toSql()
        );

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_ONE))->toSql()
        );
        self::assertCount(0, $rows);
    }

    public function test_truncate_continue_identity() : void
    {
        $lastRow = $this->pgsqlContext()->client()->fetchOne(
            select(col('id'))->from(table(self::TABLE_ONE))->orderBy(desc(col('id')))->limit(1)->toSql()
        );
        $lastId = (int) $lastRow['id'];

        $this->pgsqlContext()->client()->execute(
            truncate_table(self::TABLE_ONE)->continueIdentity()->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_ONE)
                ->columns('name')
                ->values(literal('NewRow'))
                ->toSql()
        );

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(star())->from(table(self::TABLE_ONE))->toSql()
        );

        self::assertSame($lastId + 1, $row['id']);
    }

    public function test_truncate_multiple_tables() : void
    {
        $this->pgsqlContext()->client()->execute(
            truncate_table(self::TABLE_ONE, self::TABLE_TWO)->toSql()
        );

        $rowsOne = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_ONE))->toSql()
        );
        self::assertCount(0, $rowsOne);

        $rowsTwo = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_TWO))->toSql()
        );
        self::assertCount(0, $rowsTwo);
    }

    public function test_truncate_restart_identity() : void
    {
        $this->pgsqlContext()->client()->execute(
            truncate_table(self::TABLE_ONE)->restartIdentity()->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_ONE)
                ->columns('name')
                ->values(literal('NewRow'))
                ->toSql()
        );

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(star())->from(table(self::TABLE_ONE))->toSql()
        );

        self::assertSame(1, $row['id']);
        self::assertSame('NewRow', $row['name']);
    }

    public function test_truncate_restrict() : void
    {
        $this->pgsqlContext()->client()->execute(
            truncate_table(self::TABLE_ONE)->restrict()->toSql()
        );

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_ONE))->toSql()
        );
        self::assertCount(0, $rows);
    }

    public function test_truncate_single_table() : void
    {
        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_ONE))->toSql()
        );
        self::assertCount(3, $rows);

        $this->pgsqlContext()->client()->execute(
            truncate_table(self::TABLE_ONE)->toSql()
        );

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_ONE))->toSql()
        );
        self::assertCount(0, $rows);

        $rowsTwo = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::TABLE_TWO))->toSql()
        );
        self::assertCount(2, $rowsTwo);
    }

    public function test_truncate_with_restart_identity_and_cascade() : void
    {
        $this->pgsqlContext()->client()->execute(
            truncate_table(self::TABLE_ONE, self::TABLE_TWO)
                ->restartIdentity()
                ->cascade()
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_ONE)
                ->columns('name')
                ->values(literal('AfterTruncate'))
                ->toSql()
        );

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(star())->from(table(self::TABLE_ONE))->toSql()
        );

        self::assertSame(1, $row['id']);
    }

    public function test_truncate_with_schema_qualified_table() : void
    {
        $this->pgsqlContext()->client()->execute('CREATE SCHEMA IF NOT EXISTS ' . self::SCHEMA_NAME);

        $this->pgsqlContext()->client()->execute(
            create()->table(self::SCHEMA_TABLE, self::SCHEMA_NAME)
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_varchar(100))->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::SCHEMA_NAME . '.' . self::SCHEMA_TABLE)
                ->columns('name')
                ->values(literal('Row 1'))
                ->values(literal('Row 2'))
                ->toSql()
        );

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::SCHEMA_TABLE, self::SCHEMA_NAME))->toSql()
        );
        self::assertCount(2, $rows);

        $this->pgsqlContext()->client()->execute(
            truncate_table(self::SCHEMA_NAME . '.' . self::SCHEMA_TABLE)->toSql()
        );

        $rows = $this->pgsqlContext()->client()->fetchAll(
            select(star())->from(table(self::SCHEMA_TABLE, self::SCHEMA_NAME))->toSql()
        );
        self::assertCount(0, $rows);
    }
}
