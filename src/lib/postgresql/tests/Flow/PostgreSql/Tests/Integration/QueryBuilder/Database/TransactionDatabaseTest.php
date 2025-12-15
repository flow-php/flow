<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    begin,
    col,
    column,
    commit,
    create,
    data_type_decimal,
    data_type_serial,
    data_type_varchar,
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

final class TransactionDatabaseTest extends DatabaseTestCase
{
    private const TABLE_ACCOUNTS = 'flow_postgres_accounts';

    protected function setUp() : void
    {
        parent::setUp();

        $this->execute(
            create()->table(self::TABLE_ACCOUNTS)
                ->column(column('id', data_type_serial()))
                ->column(column('name', data_type_varchar(100))->notNull())
                ->column(column('balance', data_type_decimal(10, 2))->default(0))
                ->constraint(primary_key('id'))
                ->toSql()
        );

        $this->execute(
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
        $this->execute(rollback()->toSql());
        $this->dropTableIfExists(self::TABLE_ACCOUNTS);

        parent::tearDown();
    }

    public function test_begin_and_commit() : void
    {
        $this->execute(begin()->toSql());

        $updateQuery = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(1500))
            ->where(eq(col('name'), literal('Account A')));

        $this->execute($updateQuery->toSql());

        $this->execute(commit()->toSql());

        $check = $this->execute(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account A')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('1500.00', $row['balance']);
    }

    public function test_begin_and_rollback() : void
    {
        $checkBefore = $this->execute(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account A')))
                ->toSql()
        );
        $beforeRow = $this->fetchOne($checkBefore);
        $originalBalance = $beforeRow['balance'];

        $this->execute(begin()->toSql());

        $updateQuery = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(9999))
            ->where(eq(col('name'), literal('Account A')));

        $this->execute($updateQuery->toSql());

        $this->execute(rollback()->toSql());

        $check = $this->execute(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account A')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame($originalBalance, $row['balance']);
    }

    public function test_begin_with_isolation_level() : void
    {
        $this->execute(begin()->isolationLevel(IsolationLevel::SERIALIZABLE)->toSql());

        $selectQuery = select(star())->from(table(self::TABLE_ACCOUNTS));
        $result = $this->execute($selectQuery->toSql());

        self::assertNotFalse($result);
        $rows = $this->fetchAll($result);
        self::assertCount(2, $rows);

        $this->execute(commit()->toSql());
    }

    public function test_begin_with_read_only() : void
    {
        $this->execute(begin()->readOnly()->toSql());

        $selectQuery = select(star())->from(table(self::TABLE_ACCOUNTS));
        $result = $this->execute($selectQuery->toSql());

        self::assertNotFalse($result);
        self::assertCount(2, $this->fetchAll($result));

        $this->execute(commit()->toSql());
    }

    public function test_rollback_to_savepoint() : void
    {
        $this->execute(begin()->toSql());

        $updateQuery1 = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(750))
            ->where(eq(col('name'), literal('Account A')));
        $this->execute($updateQuery1->toSql());

        $this->execute(savepoint('sp_rollback')->toSql());

        $updateQuery2 = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(9999))
            ->where(eq(col('name'), literal('Account A')));
        $this->execute($updateQuery2->toSql());

        $this->execute(rollback()->toSavepoint('sp_rollback')->toSql());

        $check = $this->execute(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account A')))
                ->toSql()
        );
        $row = $this->fetchOne($check);
        self::assertSame('750.00', $row['balance']);

        $this->execute(commit()->toSql());
    }

    public function test_savepoint_and_release() : void
    {
        $this->execute(begin()->toSql());

        $updateQuery1 = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(800))
            ->where(eq(col('name'), literal('Account A')));
        $this->execute($updateQuery1->toSql());

        $this->execute(savepoint('sp1')->toSql());

        $updateQuery2 = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(600))
            ->where(eq(col('name'), literal('Account B')));
        $this->execute($updateQuery2->toSql());

        $this->execute(release_savepoint('sp1')->toSql());
        $this->execute(commit()->toSql());

        $checkA = $this->execute(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account A')))
                ->toSql()
        );
        $rowA = $this->fetchOne($checkA);
        self::assertSame('800.00', $rowA['balance']);

        $checkB = $this->execute(
            select(col('balance'))
                ->from(table(self::TABLE_ACCOUNTS))
                ->where(eq(col('name'), literal('Account B')))
                ->toSql()
        );
        $rowB = $this->fetchOne($checkB);
        self::assertSame('600.00', $rowB['balance']);
    }
}
