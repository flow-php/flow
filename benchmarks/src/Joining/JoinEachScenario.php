<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Joining;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\ref;
use function Flow\Floe\DSL\from_floe;
use function Flow\Types\DSL\type_string;

final readonly class JoinEachScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->withEntry('seller_id', ref('seller_id')->cast(type_string()))
            ->joinEach(new SellersDataFrameFactory(Datasets::sellers($this->rows)->parquet()), join_on([
                'seller_id' => 'id',
            ], join_prefix: 'sellers_'))
            ->run();
    }
}
