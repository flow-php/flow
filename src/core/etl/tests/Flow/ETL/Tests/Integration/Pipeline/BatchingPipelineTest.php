<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use Flow\ETL\Config;
use Flow\ETL\DSL\From;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BatchingPipeline;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class BatchingPipelineTest extends TestCase
{
    public function test_batching_rows() : void
    {
        $pipeline = new BatchingPipeline(new SynchronousPipeline(), size: 10);
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

    public function test_that_rows_are_not_lost() : void
    {
        $pipeline = new BatchingPipeline(new SynchronousPipeline(), $batchSize = 7);
        $pipeline->source(From::chain(
            From::array([
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
            ])
        ));

        $this->assertEquals(
            [
                [
                    ['id' => 1],
                    ['id' => 2],
                    ['id' => 3],
                    ['id' => 4],
                    ['id' => 5],
                    ['id' => 6],
                    ['id' => 7],
                ],
                [
                    ['id' => 8],
                    ['id' => 9],
                    ['id' => 10],
                ],
            ],
            \array_map(
                static fn (Rows $r) => $r->toArray(),
                \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
            )
        );
    }

    public function test_using_bigger_batch_size_than_total_number_of_rows() : void
    {
        $pipeline = new BatchingPipeline(new SynchronousPipeline(), size: 11);
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

    public function test_using_smaller_batch_size_than_total_number_of_rows() : void
    {
        $pipeline = new BatchingPipeline(new SynchronousPipeline(), size: 5);
        $pipeline->source(From::chain(
            From::array([
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
            ])
        ));

        $this->assertCount(
            2,
            \iterator_to_array($pipeline->process(new FlowContext(Config::default())))
        );
    }
}
