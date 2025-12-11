<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    agg_count,
    col,
    column,
    cond_or,
    create,
    delete,
    eq,
    insert,
    literal,
    primary_key,
    select,
    sql_type_integer,
    sql_type_serial,
    sql_type_text,
    sql_type_timestamp,
    sql_type_varchar,
    star,
    table
};

final class DeleteDatabaseTest extends DatabaseTestCase
{
    private const TABLE_ARCHIVE = 'flow_postgres_log_archive';

    private const TABLE_LOGS = 'flow_postgres_logs';

    protected function setUp() : void
    {
        parent::setUp();

        $this->execute(
            create()->table(self::TABLE_LOGS)
                ->column(column('id', sql_type_serial()))
                ->column(column('level', sql_type_varchar(20))->notNull())
                ->column(column('message', sql_type_text()))
                ->column(column('created_at', sql_type_timestamp()))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
            create()->table(self::TABLE_ARCHIVE)
                ->column(column('id', sql_type_serial()))
                ->column(column('log_id', sql_type_integer())->notNull())
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
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

        $this->execute(
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
        $this->dropTableIfExists(self::TABLE_ARCHIVE);
        $this->dropTableIfExists(self::TABLE_LOGS);

        parent::tearDown();
    }

    public function test_delete_all() : void
    {
        $query = delete()
            ->from(self::TABLE_ARCHIVE);

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(2, $this->affectedRows($result));

        $check = $this->execute(
            select(agg_count(star())->as('cnt'))
                ->from(table(self::TABLE_ARCHIVE))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('0', $row['cnt']);
    }

    public function test_delete_with_multiple_conditions() : void
    {
        $query = delete()
            ->from(self::TABLE_LOGS)
            ->where(
                cond_or(
                    eq(col('level'), literal('INFO')),
                    eq(col('level'), literal('WARNING'))
                )
            );

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(2, $this->affectedRows($result));
    }

    public function test_delete_with_returning() : void
    {
        $query = delete()
            ->from(self::TABLE_LOGS)
            ->where(eq(col('level'), literal('ERROR')))
            ->returning(col('id'), col('level'), col('message'));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $row = $this->fetchOne($result);
        self::assertSame('ERROR', $row['level']);
        self::assertSame('Error occurred', $row['message']);
    }

    public function test_delete_with_returning_all() : void
    {
        $this->execute(
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

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        $row = $this->fetchOne($result);
        self::assertArrayHasKey('id', $row);
        self::assertArrayHasKey('level', $row);
        self::assertArrayHasKey('message', $row);
        self::assertArrayHasKey('created_at', $row);
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

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(2, $this->affectedRows($result));
    }

    public function test_delete_with_where() : void
    {
        $query = delete()
            ->from(self::TABLE_LOGS)
            ->where(eq(col('level'), literal('DEBUG')));

        $result = $this->execute($query->toSql());

        self::assertNotFalse($result);
        self::assertSame(2, $this->affectedRows($result));

        $check = $this->execute(
            select(agg_count(star())->as('cnt'))
                ->from(table(self::TABLE_LOGS))
                ->where(eq(col('level'), literal('DEBUG')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('0', $row['cnt']);
    }
}
