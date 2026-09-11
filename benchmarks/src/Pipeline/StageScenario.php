<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Paths;

use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\to_floe;
use function uniqid;

/**
 * Rungs 2 and 3 of the ladder. They project five columns of the same five types as rung 4, but rung
 * 4's fifth column is the concatenated contact string where these write email — so the r4 - r3 delta
 * carries a small sink-payload difference on top of the transform. That residual is stated, not
 * hidden, and no delta is gate-able.
 *
 * Subtractability across the three bench classes holds only because every one of them uses stock
 * runner settings, BenchmarkConfig::builder() and OrdersSchema::of().
 */
final readonly class StageScenario
{
    public function __construct(
        private Source $source,
        private Stage $stage,
        private int $rows,
    ) {}

    public function run(): void
    {
        $frame = data_frame(BenchmarkConfig::builder())
            ->read((new SourceExtractor($this->source, SchemaMode::declared, $this->rows))->extractor())
            ->select('order_id', 'seller_id', 'created_at', 'customer', 'email');

        match ($this->stage) {
            Stage::read_select => $frame->run(),
            Stage::read_select_write => $frame->write(to_floe(
                Paths::var() . '/stage_' . $this->source->value . '_' . uniqid('', true) . '.floe',
            ))->run(),
        };
    }
}
