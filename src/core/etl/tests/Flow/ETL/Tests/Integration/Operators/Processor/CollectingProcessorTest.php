<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Operators\Processor;

use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Tests\Context\ExecutedPipelines;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function iterator_to_array;

final class CollectingProcessorTest extends FlowTestCase
{
    public function test_collecting(): void
    {
        $source = from_all(
            from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ]),
            from_array([
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ]),
            from_array([
                ['id' => 11],
                ['id' => 12],
                ['id' => 13],
            ]),
        );
        $steps = [];
        $steps[] = new CollectingProcessor();

        static::assertCount(1, iterator_to_array(ExecutedPipelines::of($source, $steps, flow_context(config()))));
    }
}
