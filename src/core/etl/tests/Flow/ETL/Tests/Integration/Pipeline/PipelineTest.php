<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use function Flow\ETL\DSL\{from_rows, rows};
use Flow\ETL\{GroupBy, Loader, Pipeline, Tests\FlowTestCase, Transformer};
use Flow\ETL\Processor\{CollectingProcessor, GroupByProcessor};

final class PipelineTest extends FlowTestCase
{
    public function test_getting_steps_from_pipeline() : void
    {
        $pipeline = new Pipeline(from_rows(rows()));
        $pipeline->add($transformer1 = $this->createMock(Transformer::class));
        $pipeline->add($groupBy = new GroupByProcessor(new GroupBy()));
        $pipeline->add($transformer2 = $this->createMock(Transformer::class));
        $pipeline->add($collecting = new CollectingProcessor());
        $pipeline->add($loader = $this->createMock(Loader::class));

        self::assertSame(
            [
                $transformer1,
                $groupBy,
                $transformer2,
                $collecting,
                $loader,
            ],
            $pipeline->stages()->steps()
        );
    }
}
