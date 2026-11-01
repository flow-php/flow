<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Paths;
use Flow\ETL\IO\Extractor;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\Floe\DSL\to_floe;
use function uniqid;

/**
 * Every comparable subject runs through here so sources differ only by their read leg.
 */
final readonly class CanonicalPipeline
{
    private string $output;

    public function __construct(
        private Extractor $extractor,
        string $label,
    ) {
        $this->output = Paths::var() . '/pipeline_' . $label . '_' . uniqid('', true) . '.floe';
    }

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read($this->extractor)
            ->withColumn('contact', ref('customer')->concat(lit(' <'), ref('email'), lit('>')))
            ->filter(ref('created_at')->isNotNull())
            ->select('order_id', 'seller_id', 'created_at', 'customer', 'contact')
            ->write(to_floe($this->output))
            ->run();
    }

    public function output(): string
    {
        return $this->output;
    }
}
