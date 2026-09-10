<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Doctrine;

use Flow\Benchmarks\Datasets\Datasets;
use Flow\Benchmarks\Pipeline\CanonicalPipeline;
use Flow\Benchmarks\Pipeline\OrdersSchema;
use Flow\Benchmarks\Pipeline\ServiceSource;

use function Flow\ETL\Adapter\Doctrine\from_dbal_key_set_qb;
use function Flow\ETL\Adapter\Doctrine\pagination_key_asc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_set;
use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;

/**
 * The canonical pipeline over a live DBAL connection. It does not degrade when the container is
 * down - DoctrineConnection::open() throws, because a subject that silently measures nothing is
 * worse than no subject.
 *
 * DBAL declares from driver metadata and never samples, so like parquet and floe it has no inferred arm.
 */
final readonly class DoctrinePipelineScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function table(): string
    {
        return 'benchmark_pipeline_doctrine_' . $this->rows;
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

        (new CanonicalPipeline(
            from_dbal_key_set_qb(
                $connection,
                $connection->createQueryBuilder()->select('*')->from($this->table()),
                pagination_key_set(pagination_key_asc('order_id')),
            )
                ->withPageSize(1000)
                ->withSchema(OrdersSchema::ofService(ServiceSource::doctrine)),
            'doctrine',
        ))->run();

        $connection->close();
    }

    public function dropTable(): void
    {
        $connection = DoctrineConnection::open();
        DoctrineConnection::dropTable($connection, $this->table());
        $connection->close();
    }
}
