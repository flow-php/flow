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
use RuntimeException;

use function Flow\ETL\Adapter\PostgreSql\to_pgsql_table;
use function Flow\ETL\Adapter\PostgreSql\to_pgsql_transaction;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ignore_error_handler;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_transformation;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\create;

final class PostgreSqlTransactionSinkTest extends IntegrationTestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this->client->execute(
            create()->table('flow_pgsql_tx_drain')->column(column('id', column_type_integer())->primaryKey()),
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
