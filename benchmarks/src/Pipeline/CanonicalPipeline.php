<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\Benchmarks\Datasets\Paths;
use Flow\ETL\Extractor;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\Floe\DSL\to_floe;
use function uniqid;

/**
 * The one pipeline shape every source is measured through, so a source's number differs only by its
 * read leg. Every subject that claims to be comparable with the others must run through here.
 *
 * withEntry is the only verb that adds a column, so it forces schema widening at plan-bind time.
 * filter is deliberately non-selective - created_at is never null - so the row count reaching the sink
 * cannot move for a second reason. select re-converges every source onto the same five columns, so the
 * sink does identical work whatever was read. Floe stores typed values, adding no stringification the
 * source had not already paid.
 *
 * The sink path is unique per call because a fixed path re-run across iterations and revolutions would
 * measure overwrite behaviour.
 */
final readonly class CanonicalPipeline
{
    private string $sink;

    public function __construct(
        private Extractor $extractor,
        string $label,
    ) {
        $this->sink = Paths::var() . '/pipeline_' . $label . '_' . uniqid('', true) . '.floe';
    }

    public function run(): void
    {
        data_frame(BenchmarkConfig::builder())
            ->read($this->extractor)
            ->withEntry('contact', ref('customer')->concat(lit(' <'), ref('email'), lit('>')))
            ->filter(ref('created_at')->isNotNull())
            ->select('order_id', 'seller_id', 'created_at', 'customer', 'contact')
            ->write(to_floe($this->sink))
            ->run();
    }

    public function sink(): string
    {
        return $this->sink;
    }
}
