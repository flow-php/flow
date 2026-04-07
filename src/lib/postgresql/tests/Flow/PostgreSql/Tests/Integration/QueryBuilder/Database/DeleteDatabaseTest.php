<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    agg_count,
    col,
    column,
    column_type_integer,
    column_type_serial,
    column_type_text,
    column_type_timestamp,
    column_type_varchar,
    create,
    delete,
    eq,
    insert,
    literal,
    or_,
    primary_key,
    select,
    star,
    table
};
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class DeleteDatabaseTest extends PostgreSqlTestCase
{
    private const SCHEMA_NAME = 'flow_postgres_test_delete_schema';

    private const SCHEMA_TABLE = 'flow_postgres_schema_logs';

    private const TABLE_ARCHIVE = 'flow_postgres_log_archive';

    private const TABLE_LOGS = 'flow_postgres_logs';

    protected function setUp() : void
    {
        parent::setUp();

        $this->pgsqlContext()->client()->execute(
            create()->table(self::TABLE_LOGS)
                ->column(column('id', column_type_serial()))
                ->column(column('level', column_type_varchar(20))->notNull())
                ->column(column('message', column_type_text()))
                ->column(column('created_at', column_type_timestamp()))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            create()->table(self::TABLE_ARCHIVE)
                ->column(column('id', column_type_serial()))
                ->column(column('log_id', column_type_integer())->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_LOGS)
                ->columns('level', 'message')
                ->values(literal('INFO'), literal('Application started'))
                ->values(literal('DEBUG'), literal('Debug message 1'))
                ->values(literal('DEBUG'), literal('Debug message 2'))
                ->values(literal('WARNING'), literal('Warning message'))
                ->values(literal('ERROR'), literal('Error occurred'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_ARCHIVE)
                ->columns('log_id')
                ->values(literal(1))
                ->values(literal(2))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_ARCHIVE);
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_LOGS);
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA_NAME);

        parent::tearDown();
    }

    public function test_delete_all() : void
    {
        $query = delete()
            ->from(self::TABLE_ARCHIVE);

        self::assertSame(2, $this->pgsqlContext()->client()->execute($query->toSql()));

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count(star())->as('cnt'))
                ->from(table(self::TABLE_ARCHIVE))
                ->toSql()
        );
        self::assertSame(0, $row['cnt']);
    }

    public function test_delete_with_multiple_conditions() : void
    {
        $query = delete()
            ->from(self::TABLE_LOGS)
            ->where(
                or_(
                    eq(col('level'), literal('INFO')),
                    eq(col('level'), literal('WARNING'))
                )
            );

        self::assertSame(2, $this->pgsqlContext()->client()->execute($query->toSql()));
    }

    public function test_delete_with_returning() : void
    {
        $query = delete()
            ->from(self::TABLE_LOGS)
            ->where(eq(col('level'), literal('ERROR')))
            ->returning(col('id'), col('level'), col('message'));

        $row = $this->pgsqlContext()->client()->fetchOne($query->toSql());

        self::assertSame('ERROR', $row['level']);
        self::assertSame('Error occurred', $row['message']);
    }

    public function test_delete_with_returning_all() : void
    {
        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_LOGS)
                ->columns('level', 'message')
                ->values(literal('TRACE'), literal('Trace message'))
                ->toSql()
        );

        $query = delete()
            ->from(self::TABLE_LOGS)
            ->where(eq(col('level'), literal('TRACE')))
            ->returningAll();

        $row = $this->pgsqlContext()->client()->fetchOne($query->toSql());

        self::assertArrayHasKey('id', $row);
        self::assertArrayHasKey('level', $row);
        self::assertArrayHasKey('message', $row);
        self::assertArrayHasKey('created_at', $row);
    }

    public function test_delete_with_schema_qualified_table() : void
    {
        $this->pgsqlContext()->client()->execute('CREATE SCHEMA IF NOT EXISTS ' . self::SCHEMA_NAME);

        $this->pgsqlContext()->client()->execute(
            create()->table(self::SCHEMA_TABLE, self::SCHEMA_NAME)
                ->column(column('id', column_type_serial()))
                ->column(column('level', column_type_varchar(20))->notNull())
                ->column(column('message', column_type_text()))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::SCHEMA_NAME . '.' . self::SCHEMA_TABLE)
                ->columns('level', 'message')
                ->values(literal('DEBUG'), literal('Test message'))
                ->values(literal('INFO'), literal('Info message'))
                ->toSql()
        );

        $query = delete()
            ->from(self::SCHEMA_NAME . '.' . self::SCHEMA_TABLE)
            ->where(eq(col('level'), literal('DEBUG')));

        self::assertSame(1, $this->pgsqlContext()->client()->execute($query->toSql()));

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count(star())->as('cnt'))
                ->from(table(self::SCHEMA_TABLE, self::SCHEMA_NAME))
                ->toSql()
        );
        self::assertSame(1, $row['cnt']);
    }

    public function test_delete_with_using() : void
    {
        $query = delete()
            ->from(self::TABLE_LOGS)
            ->using(table(self::TABLE_ARCHIVE))
            ->where(
                eq(
                    col('id', self::TABLE_LOGS),
                    col('log_id', self::TABLE_ARCHIVE)
                )
            );

        self::assertSame(2, $this->pgsqlContext()->client()->execute($query->toSql()));
    }

    public function test_delete_with_where() : void
    {
        $query = delete()
            ->from(self::TABLE_LOGS)
            ->where(eq(col('level'), literal('DEBUG')));

        self::assertSame(2, $this->pgsqlContext()->client()->execute($query->toSql()));

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count(star())->as('cnt'))
                ->from(table(self::TABLE_LOGS))
                ->where(eq(col('level'), literal('DEBUG')))
                ->toSql()
        );
        self::assertSame(0, $row['cnt']);
    }
}
