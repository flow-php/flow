<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;

use function Flow\ETL\DSL\data_frame;

final readonly class ColumnMixScenario
{
    public function __construct(
        private Source $source,
        private ColumnMix $mix,
        private int $rows,
    ) {}

    /**
     * @return list<string>
     */
    public function columns(): array
    {
        return $this->mix->columns();
    }

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read((new SourceExtractor($this->source, SchemaMode::declared, $this->rows))->extractor())
            ->select(...$this->columns())
            ->run();
    }
}
