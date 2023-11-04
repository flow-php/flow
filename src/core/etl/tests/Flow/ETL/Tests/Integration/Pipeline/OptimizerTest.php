<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use PHPUnit\Framework\TestCase;

final class OptimizerTest extends TestCase
{
    public function test_adding_element_to_pipeline_when_no_optimization_is_applicable() : void
    {
        $pipeline = new SynchronousPipeline();

        $optimizedPipeline = (new Optimizer())->optimize(new KeepEntriesTransformer(ref('id')), $pipeline);

        $this->assertCount(1, $optimizedPipeline->pipes()->all());
    }
}
