<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use Flow\ETL\Adapter\PostgreSql\Tests\Context\TableRowsContext;
use Flow\ETL\Adapter\PostgreSql\Tests\Double\TransactionSpyLoader;
use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;
use Flow\ETL\DataFrame;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\ClosureThrowingLoader;
use Flow\ETL\Tests\Double\LoadThenThrowLoader;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use RuntimeException;
use Throwable;

use function Flow\ETL\Adapter\PostgreSql\to_pgsql_table;
use function Flow\ETL\Adapter\PostgreSql\to_pgsql_transaction;
use function Flow\ETL\DSL\batch_size;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ignore_error_handler;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_transformation;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\create;

final class PostgreSqlTransactionSinkTest extends IntegrationTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        foreach (['flow_pgsql_tx_primary', 'flow_pgsql_tx_mirror'] as $tableName) {
            $this->client->execute(
                create()
                    ->table($tableName)
                    ->column(column('id', column_type_integer())->primaryKey())
                    ->column(column('name', column_type_text())),
            );
        }

        $this->client->execute(
            create()->table('flow_pgsql_tx_deleted')->column(column('transaction_id', column_type_integer())),
        );
        $this->client->execute(
            create()
                ->table('flow_pgsql_tx_inserted')
                ->column(column('id', column_type_integer())->primaryKey())
                ->column(column('transaction_id', column_type_integer())),
        );
        $this->client->execute(
            create()->table('flow_pgsql_tx_drain')->column(column('id', column_type_integer())->primaryKey()),
        );
    }

    public function test_commits_every_loader_in_a_single_transaction(): void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ]))
            ->write(to_pgsql_transaction(
                $this->client,
                to_pgsql_table($this->client, 'flow_pgsql_tx_primary'),
                to_pgsql_table($this->client, 'flow_pgsql_tx_mirror'),
            ))
            ->run();

        $expected = [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']];

        static::assertSame($expected, TableRowsContext::fetchAll($this->client, 'flow_pgsql_tx_primary'));
        static::assertSame($expected, TableRowsContext::fetchAll($this->client, 'flow_pgsql_tx_mirror'));
    }

    public function test_rolls_back_all_loaders_when_one_fails(): void
    {
        df()
            ->read(from_array([['id' => 1, 'name' => 'Existing']]))
            ->write(to_pgsql_table($this->client, 'flow_pgsql_tx_mirror'))
            ->run();

        $thrown = null;

        try {
            df()
                ->read(from_array([
                    ['id' => 1, 'name' => 'Alice'],
                    ['id' => 2, 'name' => 'Bob'],
                ]))
                ->write(to_pgsql_transaction(
                    $this->client,
                    to_pgsql_table($this->client, 'flow_pgsql_tx_primary'),
                    to_pgsql_table($this->client, 'flow_pgsql_tx_mirror'),
                ))
                ->run();
        } catch (Throwable $e) {
            $thrown = $e;
        }

        // id=1 already exists in the mirror table; the whole batch must roll back
        static::assertInstanceOf(Throwable::class, $thrown);
        static::assertSame([], TableRowsContext::fetchAll($this->client, 'flow_pgsql_tx_primary'));
        static::assertSame(
            [['id' => 1, 'name' => 'Existing']],
            TableRowsContext::fetchAll($this->client, 'flow_pgsql_tx_mirror'),
        );
    }

    public function test_a_batched_transaction_sink_commits_once_per_batch(): void
    {
        $baseline = $this->client->getTransactionNestingLevel();
        $del = new TransactionSpyLoader($this->client);
        $ins = new TransactionSpyLoader($this->client);
        $rows = [];

        for ($id = 1; $id <= 5; $id++) {
            $rows[] = ['transaction_id' => 7, 'id' => $id];
        }

        for ($id = 6; $id <= 8; $id++) {
            $rows[] = ['transaction_id' => 9, 'id' => $id];
        }

        df()
            ->read(from_array($rows))
            ->batchBy(ref('transaction_id'), 5)
            ->write(to_pgsql_transaction(
                $this->client,
                to_transformation(new CallbackTransformation(
                    static fn(DataFrame $df): DataFrame => $df->select('transaction_id'),
                ), $del),
                to_transformation(batch_size(3), $ins),
            ))
            ->run();

        // one transaction per source batch (tx 7, tx 9), and the 2 rows batch_size(3) still buffers at the end of the
        // stream are delivered in the final transaction the drain opens
        static::assertSame(
            [['rows' => 5, 'nestingLevel' => $baseline + 1], ['rows' => 3, 'nestingLevel' => $baseline + 1]],
            $del->deliveries,
        );
        static::assertSame(
            [
                ['rows' => 3, 'nestingLevel' => $baseline + 1],
                ['rows' => 3, 'nestingLevel' => $baseline + 1],
                ['rows' => 2, 'nestingLevel' => $baseline + 1],
            ],
            $ins->deliveries,
        );
        static::assertSame([$baseline + 1], $del->closureNestingLevels);
        static::assertSame([$baseline + 1], $ins->closureNestingLevels);
    }

    public function test_every_batch_and_the_drain_commit_in_their_own_transaction(): void
    {
        df()
            ->read(from_array([
                ['transaction_id' => 7, 'id' => 1],
                ['transaction_id' => 7, 'id' => 2],
                ['transaction_id' => 7, 'id' => 3],
                ['transaction_id' => 7, 'id' => 4],
                ['transaction_id' => 7, 'id' => 5],
                ['transaction_id' => 9, 'id' => 6],
                ['transaction_id' => 9, 'id' => 7],
                ['transaction_id' => 9, 'id' => 8],
            ]))
            ->batchBy(ref('transaction_id'), 5)
            ->write(to_pgsql_transaction(
                $this->client,
                to_transformation(
                    new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->select('transaction_id')),
                    to_pgsql_table($this->client, 'flow_pgsql_tx_deleted'),
                ),
                to_transformation(batch_size(3), to_pgsql_table($this->client, 'flow_pgsql_tx_inserted')),
            ))
            ->run();

        static::assertSame(
            [[7, 7, 7, 7, 7], [9, 9, 9]],
            TableRowsContext::groupedByWritingTransaction($this->client, 'flow_pgsql_tx_deleted', 'transaction_id'),
        );
        static::assertSame(
            [[1, 2, 3], [4, 5, 6], [7, 8]],
            TableRowsContext::groupedByWritingTransaction($this->client, 'flow_pgsql_tx_inserted', 'id'),
        );
    }

    public function test_two_sinks_on_one_connection_commit_together(): void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ]))
            ->write(to_pgsql_transaction(
                $this->client,
                to_transformation(
                    new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->select('id', 'name')),
                    to_pgsql_table($this->client, 'flow_pgsql_tx_primary'),
                ),
                to_branch(ref('id')->greaterThan(lit(0)), to_pgsql_table($this->client, 'flow_pgsql_tx_mirror')),
            ))
            ->run();

        $expected = [['id' => 1, 'name' => 'Alice'], ['id' => 2, 'name' => 'Bob']];

        static::assertSame($expected, TableRowsContext::fetchAll($this->client, 'flow_pgsql_tx_primary'));
        static::assertSame($expected, TableRowsContext::fetchAll($this->client, 'flow_pgsql_tx_mirror'));
    }

    public function test_a_rolled_back_child_keeps_writing_later_batches(): void
    {
        df()
            ->read(from_array([
                ['id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'name' => 'Bob'],
            ]))
            ->batchSize(1)
            ->onError(ignore_error_handler())
            ->write(to_pgsql_transaction(
                $this->client,
                to_transformation(
                    new ThrowWhenRowMatches('id', 1, new RuntimeException('boom')),
                    to_pgsql_table($this->client, 'flow_pgsql_tx_primary'),
                ),
                to_pgsql_table($this->client, 'flow_pgsql_tx_mirror'),
            ))
            ->run();

        // the first batch rolled back for every child and is never re-delivered; the failing child was restarted
        static::assertSame(
            [['id' => 2, 'name' => 'Bob']],
            TableRowsContext::fetchAll($this->client, 'flow_pgsql_tx_primary'),
        );
        static::assertSame(
            [['id' => 2, 'name' => 'Bob']],
            TableRowsContext::fetchAll($this->client, 'flow_pgsql_tx_mirror'),
        );
    }

    public function test_a_drain_failure_suppressed_by_the_error_handler_rolls_back_the_delivery_and_surfaces_the_failure(): void
    {
        $sink = new LoadThenThrowLoader(
            to_pgsql_table($this->client, 'flow_pgsql_tx_drain'),
            new RuntimeException('sink failed'),
        );

        // a drain failure is never offered to the handler: the drain rolls back and the user's exception surfaces
        try {
            df()
                ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
                ->onError(ignore_error_handler())
                ->batchSize(2)
                ->write(to_pgsql_transaction($this->client, to_transformation(
                    new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref('id')])),
                    $sink,
                )))
                ->run();

            static::fail('Expected the drain failure to surface');
        } catch (RuntimeException $e) {
            static::assertSame('sink failed', $e->getMessage());
        }

        static::assertSame(1, $sink->loadsCount);
        static::assertSame([], TableRowsContext::fetchAll($this->client, 'flow_pgsql_tx_drain'));
    }

    public function test_a_failure_during_the_closure_transaction_rolls_back_the_drained_delivery(): void
    {
        $thrown = null;

        try {
            df()
                ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
                ->batchSize(2)
                ->write(to_pgsql_transaction(
                    $this->client,
                    to_transformation(
                        new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref('id')])),
                        to_pgsql_table($this->client, 'flow_pgsql_tx_drain'),
                    ),
                    new ClosureThrowingLoader(new RuntimeException('closure failed')),
                ))
                ->run();
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(RuntimeException::class, $thrown);
        static::assertSame('closure failed', $thrown->getMessage());

        static::assertSame([], TableRowsContext::fetchAll($this->client, 'flow_pgsql_tx_drain'));
    }

    public function test_blocking_transformation_delivers_its_whole_stream_at_closure_inside_a_transaction(): void
    {
        $spy = new TransactionSpyLoader($this->client);
        $baseline = $this->client->getTransactionNestingLevel();

        df()
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
            ->batchSize(2)
            ->write(to_pgsql_transaction($this->client, to_transformation(
                new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref('id')])),
                $spy,
            )))
            ->run();

        static::assertSame([['rows' => 4, 'nestingLevel' => $baseline + 1]], $spy->deliveries);
        static::assertSame([$baseline + 1], $spy->closureNestingLevels);
    }

    public function test_branch_armed_with_a_blocking_transformation_delivers_at_closure_inside_a_transaction(): void
    {
        $spy = new TransactionSpyLoader($this->client);
        $baseline = $this->client->getTransactionNestingLevel();

        df()
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
            ->batchSize(2)
            ->write(to_pgsql_transaction($this->client, to_branch(
                lit(true),
                $spy,
            )->withTransformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref(
                'id',
            )])))))
            ->run();

        static::assertSame([['rows' => 4, 'nestingLevel' => $baseline + 1]], $spy->deliveries);
        static::assertSame([$baseline + 1], $spy->closureNestingLevels);
    }

    public function test_streaming_transformation_delivers_each_batch_inside_a_transaction(): void
    {
        $spy = new TransactionSpyLoader($this->client);
        $baseline = $this->client->getTransactionNestingLevel();

        df()
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
            ->batchSize(2)
            ->write(to_pgsql_transaction($this->client, to_transformation(new CallbackTransformation(
                static fn(DataFrame $df): DataFrame => $df->select('id'),
            ), $spy)))
            ->run();

        static::assertSame(
            [['rows' => 2, 'nestingLevel' => $baseline + 1], ['rows' => 2, 'nestingLevel' => $baseline + 1]],
            $spy->deliveries,
        );
        static::assertSame([$baseline + 1], $spy->closureNestingLevels);
    }
}
