<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\ETL\Rows;

use function Flow\ETL\DSL\data_frame;

final readonly class FetchScenario
{
    public function __construct(
        private Source $source,
        private int $rows,
    ) {}

    public function run(): Rows
    {
        return data_frame(BenchmarkConfig::builder())
            ->read((new SourceExtractor($this->source, SchemaMode::declared, $this->rows))->extractor())
            ->select('order_id', 'customer')
            ->fetch();
    }
}
