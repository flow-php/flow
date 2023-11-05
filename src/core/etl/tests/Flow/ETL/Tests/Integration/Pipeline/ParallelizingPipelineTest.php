<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use Flow\ETL\Config;
use Flow\ETL\DSL\From;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Pipeline\SynchronousPipeline;
use PHPUnit\Framework\TestCase;

final class ParallelizingPipelineTest extends TestCase
{
    public function test_parallelizing_pipeline() : void
    {
        $pipeline = new ParallelizingPipeline(
            new SynchronousPipeline(),
            5
        );
        $pipeline->setSource(From::array([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
            ['id' => 6],
            ['id' => 7],
            ['id' => 8],
            ['id' => 9],
            ['id' => 10],
        ]));

        $this->assertCount(
            2,
            \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
        );
    }

    public function test_parallelizing_pipeline_with_batch_size_greater_than_total_number_of_rows() : void
    {
        $pipeline = new ParallelizingPipeline(
            new SynchronousPipeline(),
            15
        );
        $pipeline->setSource(From::array([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
            ['id' => 6],
            ['id' => 7],
            ['id' => 8],
            ['id' => 9],
            ['id' => 10],
        ]));

        $this->assertCount(
            1,
            \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
        );
    }
}
