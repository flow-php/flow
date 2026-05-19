<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Client\Exception\TransactionException;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;
use RuntimeException;

use function Flow\PostgreSql\DSL\agg_count;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\insert;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class TransactionTest extends PostgreSqlTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->temporaryTable('test_transaction')
                    ->column(column('id', column_type_serial()))
                    ->column(column('name', column_type_text()))
                    ->constraint(primary_key('id')),
            );
    }

    public function test_auto_commit_default_is_true(): void
    {
        static::assertTrue($this->pgsqlContext()->client()->isAutoCommit());
    }

    public function test_auto_commit_disabled_allows_rollback(): void
    {
        $this->pgsqlContext()->client()->setAutoCommit(false);

        $this
            ->pgsqlContext()
            ->client()
            ->execute(insert()->into('test_transaction')->columns('name')->values(literal('will be rolled back')));

        $this->pgsqlContext()->client()->rollBack();

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));
        static::assertSame(0, $count);

        static::assertSame(0, $this->pgsqlContext()->client()->getTransactionNestingLevel());
    }

    public function test_auto_commit_disabled_starts_transaction(): void
    {
        static::assertSame(0, $this->pgsqlContext()->client()->getTransactionNestingLevel());

        $this->pgsqlContext()->client()->setAutoCommit(false);

        static::assertFalse($this->pgsqlContext()->client()->isAutoCommit());
        static::assertSame(1, $this->pgsqlContext()->client()->getTransactionNestingLevel());

        $this
            ->pgsqlContext()
            ->client()
            ->execute(insert()->into('test_transaction')->columns('name')->values(literal('in transaction')));

        $this->pgsqlContext()->client()->setAutoCommit(true);
        static::assertTrue($this->pgsqlContext()->client()->isAutoCommit());
        static::assertSame(0, $this->pgsqlContext()->client()->getTransactionNestingLevel());

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));
        static::assertSame(1, $count);
    }

    public function test_commit_without_transaction_throws(): void
    {
        $this->expectException(TransactionException::class);
        $this->expectExceptionMessage('no active transaction');

        $this->pgsqlContext()->client()->commit();
    }

    public function test_deeply_nested_transactions(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->transaction(static function ($client): void {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('level1')));

                $client->transaction(static function ($client): void {
                    $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('level2')));

                    $client->transaction(static function ($client): void {
                        $client->execute(
                            insert()->into('test_transaction')->columns('name')->values(literal('level3')),
                        );
                    });
                });
            });

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));
        static::assertSame(3, $count);
    }

    public function test_explicit_begin_commit(): void
    {
        $this->pgsqlContext()->client()->beginTransaction();
        $this
            ->pgsqlContext()
            ->client()
            ->execute(insert()->into('test_transaction')->columns('name')->values(literal('explicit')));
        $this->pgsqlContext()->client()->commit();

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));
        static::assertSame(1, $count);
    }

    public function test_explicit_begin_rollback(): void
    {
        $this->pgsqlContext()->client()->beginTransaction();
        $this
            ->pgsqlContext()
            ->client()
            ->execute(insert()->into('test_transaction')->columns('name')->values(literal('will_rollback')));
        $this->pgsqlContext()->client()->rollBack();

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));
        static::assertSame(0, $count);
    }

    public function test_multiple_operations_in_transaction(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->transaction(static function ($client): void {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('first')));
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('second')));
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('third')));
            });

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));

        static::assertSame(3, $count);
    }

    public function test_nested_transaction_commits(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->transaction(static function ($client): void {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('outer')));

                $client->transaction(static function ($client): void {
                    $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('inner')));
                });
            });

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));
        static::assertSame(2, $count);
    }

    public function test_nested_transaction_inner_rollback(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->transaction(static function ($client): void {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('outer')));

                try {
                    $client->transaction(static function ($client): void {
                        $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('inner')));

                        throw new RuntimeException('Inner failure');
                    });
                } catch (RuntimeException) {
                }

                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('after inner')));
            });

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));
        static::assertSame(2, $count);

        $names = $this
            ->pgsqlContext()
            ->client()
            ->fetchAll(select(col('name'))->from(table('test_transaction'))->orderBy(asc(col('id'))));
        static::assertSame('outer', $names[0]['name']);
        static::assertSame('after inner', $names[1]['name']);
    }

    public function test_nested_transaction_outer_rollback_includes_inner(): void
    {
        try {
            $this
                ->pgsqlContext()
                ->client()
                ->transaction(static function ($client): void {
                    $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('outer')));

                    $client->transaction(static function ($client): void {
                        $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('inner')));
                    });

                    throw new RuntimeException('Outer failure');
                });
        } catch (RuntimeException) {
        }

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));
        static::assertSame(0, $count);
    }

    public function test_nesting_level_tracking(): void
    {
        static::assertSame(0, $this->pgsqlContext()->client()->getTransactionNestingLevel());

        $this->pgsqlContext()->client()->beginTransaction();
        static::assertSame(1, $this->pgsqlContext()->client()->getTransactionNestingLevel());

        $this->pgsqlContext()->client()->beginTransaction();
        static::assertSame(2, $this->pgsqlContext()->client()->getTransactionNestingLevel());

        $this->pgsqlContext()->client()->commit();
        static::assertSame(1, $this->pgsqlContext()->client()->getTransactionNestingLevel());

        $this->pgsqlContext()->client()->rollBack();
        static::assertSame(0, $this->pgsqlContext()->client()->getTransactionNestingLevel());
    }

    public function test_rollback_without_transaction_throws(): void
    {
        $this->expectException(TransactionException::class);
        $this->expectExceptionMessage('no active transaction');

        $this->pgsqlContext()->client()->rollBack();
    }

    public function test_set_auto_commit_same_value_does_nothing(): void
    {
        static::assertTrue($this->pgsqlContext()->client()->isAutoCommit());
        static::assertSame(0, $this->pgsqlContext()->client()->getTransactionNestingLevel());

        $this->pgsqlContext()->client()->setAutoCommit(true);

        static::assertTrue($this->pgsqlContext()->client()->isAutoCommit());
        static::assertSame(0, $this->pgsqlContext()->client()->getTransactionNestingLevel());
    }

    public function test_transaction_can_query_within(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->transaction(static function ($client) {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('query test')));

                return $client->fetchScalarInt(select(agg_count())->from(table('test_transaction')));
            });

        static::assertSame(1, $result);
    }

    public function test_transaction_commits_on_success(): void
    {
        $this
            ->pgsqlContext()
            ->client()
            ->transaction(static function ($client): void {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('committed')));
            });

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));

        static::assertSame(1, $count);
    }

    public function test_transaction_returns_callback_value(): void
    {
        $result = $this
            ->pgsqlContext()
            ->client()
            ->transaction(static function ($client) {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('test')));

                return 'success';
            });

        static::assertSame('success', $result);
    }

    public function test_transaction_rollbacks_on_exception(): void
    {
        try {
            $this
                ->pgsqlContext()
                ->client()
                ->transaction(static function ($client): void {
                    $client->execute(
                        insert()->into('test_transaction')->columns('name')->values(literal('will be rolled back')),
                    );

                    throw new RuntimeException('Simulated failure');
                });
        } catch (RuntimeException) {
        }

        $count = $this
            ->pgsqlContext()
            ->client()
            ->fetchScalarInt(select(agg_count())->from(table('test_transaction')));

        static::assertSame(0, $count);
    }
}
