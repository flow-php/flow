<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Window;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Datasets;
use Flow\Benchmarks\Partitioning\PartitionCardinality;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\window;
use function Flow\Floe\DSL\from_floe;

final readonly class RankScenario
{
    public function __construct(
        private int $rows,
        private PartitionCardinality $cardinality,
    ) {}

    public function run(): void
    {
        $partitioning = new WindowPartitioning($this->cardinality);

        $partitioning
            ->derive(data_frame(BenchmarkConfig::builder())->read(from_floe(Datasets::orders($this->rows)->floe())))
            ->withEntry(
                'rank',
                rank()->over(window()->partitionBy($partitioning->reference())->orderBy(ref('created_at'))),
            )
            ->run();
    }
}
