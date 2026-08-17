<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\batches;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\write_with_retries;
use function Flow\Floe\DSL\from_floe;

/**
 * Rows are re-batched to one per Rows because a floe file arrives pre-chunked, which would hide the
 * BatchSizeOptimization that from_csv()/from_json() sources trigger.
 */
final readonly class DoctrineWrappedWriteScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function table(): string
    {
        return 'benchmark_orders_dbal_wrapped_write_' . $this->rows;
    }

    public function setUp(): void
    {
        Datasets::orders($this->rows)->floe();

        $connection = DoctrineConnection::open();
        DoctrineConnection::dropTable($connection, $this->table());
        DoctrineConnection::createTable($connection, $this->table());
        $connection->close();
    }

    public function run(): void
    {
        $connection = DoctrineConnection::open();

        data_frame()
            ->read(batches(from_floe(Datasets::orders($this->rows)->floe()), 1))
            ->write(write_with_retries(to_dbal_table_insert($connection, $this->table())))
            ->run();

        $connection->close();
    }

    public function dropTable(): void
    {
        $connection = DoctrineConnection::open();
        DoctrineConnection::dropTable($connection, $this->table());
        $connection->close();
    }
}
