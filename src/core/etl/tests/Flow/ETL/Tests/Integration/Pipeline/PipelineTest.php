<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\GroupBy;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\rows;

final class PipelineTest extends FlowTestCase
{
    public function test_getting_steps_from_pipeline(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $pipeline->add($transformer1 = $this->createStub(Transformer::class));
        $pipeline->add($groupBy = new GroupByAggregationProcessor(new GroupBy(), new Buckets(new MemoryBuckets())));
        $pipeline->add($transformer2 = $this->createStub(Transformer::class));
        $pipeline->add($collecting = new CollectingProcessor());
        $pipeline->add($loader = $this->createStub(Loader::class));

        static::assertSame(
            [
                $transformer1,
                $groupBy,
                $transformer2,
                $collecting,
                $loader,
            ],
            $pipeline->segments()->steps(),
        );
    }
}
