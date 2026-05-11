<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use Flow\ETL\Pipeline;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;

final class OptimizerTest extends FlowTestCase
{
    public function test_adding_element_to_pipeline_when_no_optimization_is_applicable(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));

        $optimizedPipeline = (new Optimizer())->optimize(new SelectEntriesTransformer(ref('id')), $pipeline);

        static::assertCount(1, $optimizedPipeline->segments()->steps());
    }
}
