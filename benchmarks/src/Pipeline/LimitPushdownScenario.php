<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;

use function Flow\ETL\DSL\data_frame;

/**
 * A null limit runs the same pipeline with no limit() in the plan, so the pair prices what a
 * limit left in the plan costs on a local source. The remote half of the question needs
 * push-down, which this engine does not have yet.
 */
final readonly class LimitPushdownScenario
{
    public function __construct(
        private Source $source,
        private ?int $limit,
        private int $rows,
    ) {}

    public function limit(): ?int
    {
        return $this->limit;
    }

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read((new SourceExtractor($this->source, SchemaMode::declared, $this->rows))->extractor())
            ->select('order_id', 'seller_id', 'created_at', 'customer', 'email')
            ->limit($this->limit)
            ->run();
    }
}
