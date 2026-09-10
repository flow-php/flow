<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Partitioning;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\partition_by;

/**
 * The one partitioned write, shared by the timed write subject and the untimed tree the read
 * subjects consume, so the two can never drift into measuring different trees.
 */
final readonly class PartitionedOrders
{
    public function __construct(
        private PartitionCardinality $cardinality,
        private int $rows,
    ) {}

    public function writeTo(string $root): void
    {
        $this->cardinality
            ->derive(
                data_frame(BenchmarkConfig::builder())->read(from_parquet(Datasets::orders($this->rows)->parquet())),
            )
            ->select('order_id', 'seller_id', 'created_at', 'customer', 'day')
            ->write(to_csv($root . '/orders.csv')->partitionBy(partition_by($this->cardinality->column())))
            ->run();
    }
}
