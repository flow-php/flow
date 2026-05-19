<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\begin;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_decimal;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_varchar;
use function Flow\PostgreSql\DSL\commit;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\release_savepoint;
use function Flow\PostgreSql\DSL\rollback;
use function Flow\PostgreSql\DSL\savepoint;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\update;

final class TransactionDatabaseTest extends PostgreSqlTestCase
{
    private const TABLE_ACCOUNTS = 'flow_postgres_accounts';

    protected function setUp(): void
    {
        parent::setUp();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table(self::TABLE_ACCOUNTS)
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_varchar(100))->notNull())
                    ->column(column('balance', column_type_decimal(10, 2))->default(0))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                insert()
                    ->into(self::TABLE_ACCOUNTS)
                    ->columns('name', 'balance')
                    ->values(literal('Account A'), literal(1000))
                    ->values(literal('Account B'), literal(500))
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->client()->execute(rollback()->toSql());
        $this->pgsqlContext()->dropTableIfExists(self::TABLE_ACCOUNTS);

        parent::tearDown();
    }

    public function test_begin_and_commit(): void
    {
        $this->pgsqlContext()->client()->execute(begin()->toSql());

        $updateQuery = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(1500))
            ->where(eq(col('name'), literal('Account A')));

        $this->pgsqlContext()->client()->execute($updateQuery->toSql());

        $this->pgsqlContext()->client()->execute(commit()->toSql());

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('balance'))
                    ->from(table(self::TABLE_ACCOUNTS))
                    ->where(eq(col('name'), literal('Account A')))
                    ->toSql(),
            );
        static::assertSame('1500.00', $row['balance']);
    }

    public function test_begin_and_rollback(): void
    {
        $beforeRow = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('balance'))
                    ->from(table(self::TABLE_ACCOUNTS))
                    ->where(eq(col('name'), literal('Account A')))
                    ->toSql(),
            );

        $this->pgsqlContext()->client()->execute(begin()->toSql());

        $updateQuery = update()
            ->update(self::TABLE_ACCOUNTS)
            ->set('balance', literal(9999))
            ->where(eq(col('name'), literal('Account A')));

        $this->pgsqlContext()->client()->execute($updateQuery->toSql());

        $this->pgsqlContext()->client()->execute(rollback()->toSql());

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('balance'))
                    ->from(table(self::TABLE_ACCOUNTS))
                    ->where(eq(col('name'), literal('Account A')))
                    ->toSql(),
            );
        static::assertSame($beforeRow['balance'], $row['balance']);
    }

    public function test_begin_with_isolation_level(): void
    {
        $this->pgsqlContext()->client()->execute(begin()->isolationLevel(IsolationLevel::SERIALIZABLE)->toSql());

        $selectQuery = select(star())->from(table(self::TABLE_ACCOUNTS));
        $rows = $this->pgsqlContext()->client()->fetchAll($selectQuery->toSql());
        static::assertCount(2, $rows);

        $this->pgsqlContext()->client()->execute(commit()->toSql());
    }

    public function test_begin_with_read_only(): void
    {
        $this->pgsqlContext()->client()->execute(begin()->readOnly()->toSql());

        $selectQuery = select(star())->from(table(self::TABLE_ACCOUNTS));
        static::assertCount(2, $this->pgsqlContext()->client()->fetchAll($selectQuery->toSql()));

        $this->pgsqlContext()->client()->execute(commit()->toSql());
    }

    public function test_rollback_to_savepoint(): void
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

        $row = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('balance'))
                    ->from(table(self::TABLE_ACCOUNTS))
                    ->where(eq(col('name'), literal('Account A')))
                    ->toSql(),
            );
        static::assertSame('750.00', $row['balance']);

        $this->pgsqlContext()->client()->execute(commit()->toSql());
    }

    public function test_savepoint_and_release(): void
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

        $rowA = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('balance'))
                    ->from(table(self::TABLE_ACCOUNTS))
                    ->where(eq(col('name'), literal('Account A')))
                    ->toSql(),
            );
        static::assertSame('800.00', $rowA['balance']);

        $rowB = $this
            ->pgsqlContext()
            ->client()
            ->fetchSingle(
                select(col('balance'))
                    ->from(table(self::TABLE_ACCOUNTS))
                    ->where(eq(col('name'), literal('Account B')))
                    ->toSql(),
            );
        static::assertSame('600.00', $rowB['balance']);
    }
}
