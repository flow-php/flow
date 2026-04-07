<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    begin,
    col,
    column,
    column_type_decimal,
    column_type_serial,
    column_type_varchar,
    commit,
    create,
    eq,
    insert,
    literal,
    primary_key,
    release_savepoint,
    rollback,
    savepoint,
    select,
    star,
    table,
    update
};

use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class TransactionDatabaseTest extends PostgreSqlTestCase
{
    private const TABLE_ACCOUNTS = 'flow_postgres_accounts';

    protected function setUp() : void
    {
        parent::setUp();

        $this->pgsqlContext()->client()->execute(
            create()->table(self::TABLE_ACCOUNTS)
                ->column(column('id', column_type_serial()))
                ->column(column('name', column_type_varchar(100))->notNull())
                ->column(column('balance', column_type_decimal(10, 2))->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            insert()
                ->into(self::TABLE_ACCOUNTS)
                ->columns('name', 'balance')
                ->values(literal('Account A'), literal(1000))
                ->values(literal('Account B'), literal(500))
                ->toSql()
        );
    }

    protected function tearDown() : void
    {
        $this->pgsqlContext()->client()->execute(rollback()->toSql());
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_ACCOUNTS);

        parent::tearDown();
    }

    public function test_begin_and_commit() : void
    {
        $this->pgsqlContext()->client()->execute(begin()->toSql());

        $updateQuery = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(1500))
            ->where(eq(col('name'), literal('Account A')));

        $this->pgsqlContext()->client()->execute($updateQuery->toSql());

        $this->pgsqlContext()->client()->execute(commit()->toSql());

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account A')))
                ->toSql()
        );
        self::assertSame('1500.00', $row['balance']);
    }

    public function test_begin_and_rollback() : void
    {
        $beforeRow = $this->pgsqlContext()->client()->fetchOne(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account A')))
                ->toSql()
        );
        $originalBalance = $beforeRow['balance'];

        $this->pgsqlContext()->client()->execute(begin()->toSql());

        $updateQuery = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(9999))
            ->where(eq(col('name'), literal('Account A')));

        $this->pgsqlContext()->client()->execute($updateQuery->toSql());

        $this->pgsqlContext()->client()->execute(rollback()->toSql());

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account A')))
                ->toSql()
        );
        self::assertSame($originalBalance, $row['balance']);
    }

    public function test_begin_with_isolation_level() : void
    {
        $this->pgsqlContext()->client()->execute(begin()->isolationLevel(IsolationLevel::SERIALIZABLE)->toSql());

        $selectQuery = select(star())->from(table(self::TABLE_ACCOUNTS));
        $rows = $this->pgsqlContext()->client()->fetchAll($selectQuery->toSql());
        self::assertCount(2, $rows);

        $this->pgsqlContext()->client()->execute(commit()->toSql());
    }

    public function test_begin_with_read_only() : void
    {
        $this->pgsqlContext()->client()->execute(begin()->readOnly()->toSql());

        $selectQuery = select(star())->from(table(self::TABLE_ACCOUNTS));
        self::assertCount(2, $this->pgsqlContext()->client()->fetchAll($selectQuery->toSql()));

        $this->pgsqlContext()->client()->execute(commit()->toSql());
    }

    public function test_rollback_to_savepoint() : void
    {
        $this->pgsqlContext()->client()->execute(begin()->toSql());

        $updateQuery1 = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(750))
            ->where(eq(col('name'), literal('Account A')));
        $this->pgsqlContext()->client()->execute($updateQuery1->toSql());

        $this->pgsqlContext()->client()->execute(savepoint('sp_rollback')->toSql());

        $updateQuery2 = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(9999))
            ->where(eq(col('name'), literal('Account A')));
        $this->pgsqlContext()->client()->execute($updateQuery2->toSql());

        $this->pgsqlContext()->client()->execute(rollback()->toSavepoint('sp_rollback')->toSql());

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account A')))
                ->toSql()
        );
        self::assertSame('750.00', $row['balance']);

        $this->pgsqlContext()->client()->execute(commit()->toSql());
    }

    public function test_savepoint_and_release() : void
    {
        $this->pgsqlContext()->client()->execute(begin()->toSql());

        $updateQuery1 = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(800))
            ->where(eq(col('name'), literal('Account A')));
        $this->pgsqlContext()->client()->execute($updateQuery1->toSql());

        $this->pgsqlContext()->client()->execute(savepoint('sp1')->toSql());

        $updateQuery2 = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(600))
            ->where(eq(col('name'), literal('Account B')));
        $this->pgsqlContext()->client()->execute($updateQuery2->toSql());

        $this->pgsqlContext()->client()->execute(release_savepoint('sp1')->toSql());
        $this->pgsqlContext()->client()->execute(commit()->toSql());

        $rowA = $this->pgsqlContext()->client()->fetchOne(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account A')))
                ->toSql()
        );
        self::assertSame('800.00', $rowA['balance']);

        $rowB = $this->pgsqlContext()->client()->fetchOne(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account B')))
                ->toSql()
        );
        self::assertSame('600.00', $rowB['balance']);
    }
}
