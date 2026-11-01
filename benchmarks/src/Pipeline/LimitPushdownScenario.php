<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;

use function Flow\ETL\DSL\data_frame;

/**
 * Null limit = same plan without limit(); the pair prices a limit on a local source (no push-down yet).
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
