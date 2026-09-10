<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\Doctrine\from_dbal_key_set_qb;
use function Flow\ETL\Adapter\Doctrine\pagination_key_asc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_set;
use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

final readonly class DoctrineReadScenario
{
    public function __construct(
        private int $rows,
        private ?int $limit = null,
    ) {}

    public function table(): string
    {
        return 'benchmark_orders_dbal_read_' . $this->rows;
    }

    public function seed(): void
    {
        $connection = DoctrineConnection::open();
        DoctrineConnection::dropTable($connection, $this->table());
        DoctrineConnection::createTable($connection, $this->table());

        data_frame()
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->write(to_dbal_table_insert($connection, $this->table()))
            ->run();

        $connection->close();
    }

    public function run(): void
    {
        $connection = DoctrineConnection::open();

        data_frame()
            ->read(from_dbal_key_set_qb(
                $connection,
                $connection->createQueryBuilder()->select('*')->from($this->table()),
                pagination_key_set(pagination_key_asc('order_id')),
            )->withBatchSize(1000))
            ->limit($this->limit)
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
