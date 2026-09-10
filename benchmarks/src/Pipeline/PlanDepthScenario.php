<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\ETL\Schema;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

/**
 * schema() answers from the plan without executing it, so this prices PlanBinder::bind() alone rather
 * than $depth withEntry calls per row.
 *
 * One call binds PLANS identical plans, because a single bind is far below timer resolution.
 */
final readonly class PlanDepthScenario
{
    private const PLANS = 100;

    public function __construct(
        private Source $source,
        private int $depth,
        private int $rows,
    ) {}

    public function bind(): Schema
    {
        $frame = data_frame(BenchmarkConfig::builder())
            ->read((new SourceExtractor($this->source, SchemaMode::declared, $this->rows))->extractor());

        for ($step = 0; $step < $this->depth; $step++) {
            $frame = $frame->withEntry('c' . $step, ref('customer')->concat(lit((string) $step)));
        }

        return $frame->schema();
    }

    public function run(): Schema
    {
        $schema = null;

        for ($plan = 0; $plan < self::PLANS; $plan++) {
            $schema = $this->bind();
        }

        return $schema;
    }
}
