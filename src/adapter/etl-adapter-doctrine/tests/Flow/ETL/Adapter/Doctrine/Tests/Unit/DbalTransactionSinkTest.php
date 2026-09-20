<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\TransactionIsolationLevel;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use Flow\ETL\Adapter\Doctrine\DbalTransaction;
use Flow\ETL\Adapter\Doctrine\Tests\Context\CommitCounter;
use Flow\ETL\Adapter\Doctrine\Tests\Context\DatabaseContext;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InsertQueryCounter;
use Flow\ETL\Adapter\Doctrine\Tests\Context\SelectQueryCounter;
use Flow\ETL\Adapter\Doctrine\Tests\Double\TransactionSpyLoader;
use Flow\ETL\DataFrame;
use Flow\ETL\Sink\Transactional;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\ClosureThrowingLoader;
use Flow\ETL\Tests\Double\LoadThenThrowLoader;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\Adapter\Doctrine\to_dbal_transaction;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ignore_error_handler;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_transformation;

final class DbalTransactionSinkTest extends FlowTestCase
{
    public function test_a_drain_failure_suppressed_by_the_error_handler_rolls_back_the_delivery_and_surfaces_the_failure(): void
    {
        $connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $databaseContext = new DatabaseContext(
            $connection,
            new InsertQueryCounter(),
            new SelectQueryCounter(),
            new CommitCounter(),
        );
        $databaseContext->createTable(new Table('tx_drain', [new Column('id', Type::getType(Types::INTEGER), [
            'notnull' => true,
        ])]));

        $sink = new LoadThenThrowLoader(
            to_dbal_table_insert($connection, 'tx_drain'),
            new RuntimeException('sink failed'),
        );

        // a drain failure is never offered to the handler: the drain rolls back and the user's exception surfaces
        try {
            df()
                ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
                ->onError(ignore_error_handler())
                ->batchSize(2)
                ->write(to_dbal_transaction($connection, to_transformation(
                    new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref('id')])),
                    $sink,
                )))
                ->run();

            static::fail('Expected the drain failure to surface');
        } catch (RuntimeException $e) {
            static::assertSame('sink failed', $e->getMessage());
        }

        static::assertSame(1, $sink->loadsCount);
        static::assertSame([], $databaseContext->selectAll('tx_drain'));
        static::assertFalse($connection->isTransactionActive());
    }

    public function test_a_failure_during_the_closure_transaction_rolls_back_the_drained_delivery(): void
    {
        $connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $databaseContext = new DatabaseContext(
            $connection,
            new InsertQueryCounter(),
            new SelectQueryCounter(),
            new CommitCounter(),
        );
        $databaseContext->createTable(new Table('tx_drain', [new Column('id', Type::getType(Types::INTEGER), [
            'notnull' => true,
        ])]));

        $closureThrowingLoader = new ClosureThrowingLoader(new RuntimeException('closure failed'));

        $thrown = null;

        try {
            df()
                ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
                ->batchSize(2)
                ->write(to_dbal_transaction(
                    $connection,
                    to_transformation(
                        new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref('id')])),
                        to_dbal_table_insert($connection, 'tx_drain'),
                    ),
                    $closureThrowingLoader,
                ))
                ->run();
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(RuntimeException::class, $thrown);
        static::assertSame('closure failed', $thrown->getMessage());
        static::assertSame(2, $closureThrowingLoader->loadsCount);
        static::assertSame([], $databaseContext->selectAll('tx_drain'));
        static::assertFalse($connection->isTransactionActive());
    }

    public function test_blocking_transformation_delivers_its_whole_stream_at_closure_inside_a_transaction(): void
    {
        $connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $spy = new TransactionSpyLoader($connection);

        df()
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
            ->batchSize(2)
            ->write(to_dbal_transaction($connection, to_transformation(
                new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref('id')])),
                $spy,
            )))
            ->run();

        static::assertSame([['rows' => 4, 'inTransaction' => true]], $spy->deliveries);
        static::assertSame([true], $spy->closureInTransaction);
    }

    public function test_branch_armed_with_a_blocking_transformation_delivers_at_closure_inside_a_transaction(): void
    {
        $connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $spy = new TransactionSpyLoader($connection);

        df()
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
            ->batchSize(2)
            ->write(to_dbal_transaction($connection, to_branch(
                lit(true),
                $spy,
            )->withTransformation(new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref(
                'id',
            )])))))
            ->run();

        static::assertSame([['rows' => 4, 'inTransaction' => true]], $spy->deliveries);
        static::assertSame([true], $spy->closureInTransaction);
    }

    public function test_isolation_level_applies_to_the_closure_transaction_and_is_restored(): void
    {
        $connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $spy = new TransactionSpyLoader($connection);
        $before = $connection->getTransactionIsolation();

        df()
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
            ->batchSize(2)
            ->write(
                new Transactional(
                    DbalTransaction::fromConnection(
                        $connection,
                    )->withIsolationLevel(TransactionIsolationLevel::SERIALIZABLE),
                    to_transformation(
                        new CallbackTransformation(static fn(DataFrame $df): DataFrame => $df->sortBy([ref('id')])),
                        $spy,
                    ),
                ),
            )
            ->run();

        static::assertSame([['rows' => 4, 'inTransaction' => true]], $spy->deliveries);
        static::assertSame($before, $connection->getTransactionIsolation());
    }

    public function test_streaming_transformation_delivers_each_batch_inside_a_transaction(): void
    {
        $connection = DriverManager::getConnection(['driver' => 'pdo_sqlite', 'memory' => true]);
        $spy = new TransactionSpyLoader($connection);

        df()
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 4], ['id' => 2]]))
            ->batchSize(2)
            ->write(to_dbal_transaction($connection, to_transformation(new CallbackTransformation(
                static fn(DataFrame $df): DataFrame => $df->select('id'),
            ), $spy)))
            ->run();

        static::assertSame(
            [['rows' => 2, 'inTransaction' => true], ['rows' => 2, 'inTransaction' => true]],
            $spy->deliveries,
        );
        static::assertSame([true], $spy->closureInTransaction);
    }
}
