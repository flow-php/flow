<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\BenchmarkConfig;
use Flow\ETL\Values\Schema;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

/**
 * schema() plans without executing, so this prices Planner::plan() alone.
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
            $frame = $frame->withColumn('c' . $step, ref('customer')->concat(lit((string) $step)));
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
