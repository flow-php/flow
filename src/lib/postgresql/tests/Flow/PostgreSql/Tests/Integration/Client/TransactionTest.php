<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use function Flow\PostgreSql\DSL\{agg_count, asc, col, insert, literal, select, table};
use Flow\PostgreSql\Client\Exception\TransactionException;

final class TransactionTest extends ClientTestCase
{
    protected function setUp() : void
    {
        parent::setUp();

        $this->client->execute('CREATE TEMP TABLE test_transaction (id serial PRIMARY KEY, name text)');
    }

    public function test_auto_commit_default_is_true() : void
    {
        self::assertTrue($this->client->isAutoCommit());
    }

    public function test_auto_commit_disabled_allows_rollback() : void
    {
        $this->client->setAutoCommit(false);

        $this->client->execute(insert()->into('test_transaction')->columns('name')->values(literal('will be rolled back')));

        $this->client->rollBack();

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));
        self::assertSame(0, $count);

        self::assertSame(0, $this->client->getTransactionNestingLevel());
    }

    public function test_auto_commit_disabled_starts_transaction() : void
    {
        self::assertSame(0, $this->client->getTransactionNestingLevel());

        $this->client->setAutoCommit(false);

        self::assertFalse($this->client->isAutoCommit());
        self::assertSame(1, $this->client->getTransactionNestingLevel());

        $this->client->execute(insert()->into('test_transaction')->columns('name')->values(literal('in transaction')));

        $this->client->setAutoCommit(true);
        self::assertTrue($this->client->isAutoCommit());
        self::assertSame(0, $this->client->getTransactionNestingLevel());

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));
        self::assertSame(1, $count);
    }

    public function test_commit_without_transaction_throws() : void
    {
        $this->expectException(TransactionException::class);
        $this->expectExceptionMessage('no active transaction');

        $this->client->commit();
    }

    public function test_deeply_nested_transactions() : void
    {
        $this->client->transaction(static function ($client) : void {
            $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('level1')));

            $client->transaction(static function ($client) : void {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('level2')));

                $client->transaction(static function ($client) : void {
                    $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('level3')));
                });
            });
        });

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));
        self::assertSame(3, $count);
    }

    public function test_explicit_begin_commit() : void
    {
        $this->client->beginTransaction();
        $this->client->execute(insert()->into('test_transaction')->columns('name')->values(literal('explicit')));
        $this->client->commit();

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));
        self::assertSame(1, $count);
    }

    public function test_explicit_begin_rollback() : void
    {
        $this->client->beginTransaction();
        $this->client->execute(insert()->into('test_transaction')->columns('name')->values(literal('will_rollback')));
        $this->client->rollBack();

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));
        self::assertSame(0, $count);
    }

    public function test_multiple_operations_in_transaction() : void
    {
        $this->client->transaction(static function ($client) : void {
            $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('first')));
            $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('second')));
            $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('third')));
        });

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));

        self::assertSame(3, $count);
    }

    public function test_nested_transaction_commits() : void
    {
        $this->client->transaction(static function ($client) : void {
            $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('outer')));

            $client->transaction(static function ($client) : void {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('inner')));
            });
        });

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));
        self::assertSame(2, $count);
    }

    public function test_nested_transaction_inner_rollback() : void
    {
        $this->client->transaction(static function ($client) : void {
            $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('outer')));

            try {
                $client->transaction(static function ($client) : void {
                    $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('inner')));

                    throw new \RuntimeException('Inner failure');
                });
            } catch (\RuntimeException) {
            }

            $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('after inner')));
        });

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));
        self::assertSame(2, $count);

        $names = $this->client->fetchAll(select(col('name'))->from(table('test_transaction'))->orderBy(asc(col('id'))));
        self::assertSame('outer', $names[0]['name']);
        self::assertSame('after inner', $names[1]['name']);
    }

    public function test_nested_transaction_outer_rollback_includes_inner() : void
    {
        try {
            $this->client->transaction(static function ($client) : void {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('outer')));

                $client->transaction(static function ($client) : void {
                    $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('inner')));
                });

                throw new \RuntimeException('Outer failure');
            });
        } catch (\RuntimeException) {
        }

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));
        self::assertSame(0, $count);
    }

    public function test_nesting_level_tracking() : void
    {
        self::assertSame(0, $this->client->getTransactionNestingLevel());

        $this->client->beginTransaction();
        self::assertSame(1, $this->client->getTransactionNestingLevel());

        $this->client->beginTransaction();
        self::assertSame(2, $this->client->getTransactionNestingLevel());

        $this->client->commit();
        self::assertSame(1, $this->client->getTransactionNestingLevel());

        $this->client->rollBack();
        self::assertSame(0, $this->client->getTransactionNestingLevel());
    }

    public function test_rollback_without_transaction_throws() : void
    {
        $this->expectException(TransactionException::class);
        $this->expectExceptionMessage('no active transaction');

        $this->client->rollBack();
    }

    public function test_set_auto_commit_same_value_does_nothing() : void
    {
        self::assertTrue($this->client->isAutoCommit());
        self::assertSame(0, $this->client->getTransactionNestingLevel());

        $this->client->setAutoCommit(true);

        self::assertTrue($this->client->isAutoCommit());
        self::assertSame(0, $this->client->getTransactionNestingLevel());
    }

    public function test_transaction_can_query_within() : void
    {
        $result = $this->client->transaction(static function ($client) {
            $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('query test')));

            return $client->fetchScalar(select(agg_count())->from(table('test_transaction')));
        });

        self::assertSame(1, $result);
    }

    public function test_transaction_commits_on_success() : void
    {
        $this->client->transaction(static function ($client) : void {
            $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('committed')));
        });

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));

        self::assertSame(1, $count);
    }

    public function test_transaction_returns_callback_value() : void
    {
        $result = $this->client->transaction(static function ($client) {
            $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('test')));

            return 'success';
        });

        self::assertSame('success', $result);
    }

    public function test_transaction_rollbacks_on_exception() : void
    {
        try {
            $this->client->transaction(static function ($client) : void {
                $client->execute(insert()->into('test_transaction')->columns('name')->values(literal('will be rolled back')));

                throw new \RuntimeException('Simulated failure');
            });
        } catch (\RuntimeException) {
        }

        $count = $this->client->fetchScalar(select(agg_count())->from(table('test_transaction')));

        self::assertSame(0, $count);
    }
}
