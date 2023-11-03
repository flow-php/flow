<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use Flow\ETL\Config;
use Flow\ETL\DSL\From;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\SynchronousPipeline;
use PHPUnit\Framework\TestCase;

final class CollectingPipelineTest extends TestCase
{
    public function test_collecting_with_batch_size() : void
    {
        $pipeline = new CollectingPipeline(new SynchronousPipeline(), 3);
        $pipeline->source(From::chain(
            From::array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ]),
            From::array([
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ])
        ));

        $this->assertCount(
            4,
            \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
        );
    }

    public function test_collecting_with_batch_size_smaller_bigger_than_total_number_of_rows() : void
    {
        $pipeline = new CollectingPipeline(new SynchronousPipeline(), 20);
        $pipeline->source(From::chain(
            From::array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ]),
            From::array([
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ])
        ));

        $this->assertCount(
            1,
            \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
        );
    }

    public function test_collecting_without_batch_size() : void
    {
        $pipeline = new CollectingPipeline(new SynchronousPipeline());
        $pipeline->source(From::chain(
            From::array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ]),
            From::array([
                ['id' => 6],
                ['id' => 7],
                ['id' => 8],
                ['id' => 9],
                ['id' => 10],
            ])
        ));

        $this->assertCount(
            1,
            \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
        );
    }
}
