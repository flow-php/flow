<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\SynchronousPipeline;
use PHPUnit\Framework\TestCase;

final class CollectingPipelineTest extends TestCase
{
    public function test_collecting() : void
    {
        $pipeline = new CollectingPipeline(new SynchronousPipeline());
        $pipeline->setSource(from_all(
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
            ])
        ));

        $this->assertCount(
            1,
            \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
        );
    }
}
