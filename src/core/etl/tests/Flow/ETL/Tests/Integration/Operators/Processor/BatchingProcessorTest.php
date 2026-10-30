<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Operators\Processor;

use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Tests\Context\ExecutedPipelines;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Rows;

use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function iterator_to_array;

final class BatchingProcessorTest extends FlowTestCase
{
    public function test_batching_rows(): void
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
        );
        $steps = [];
        $steps[] = new BatchingProcessor(new BatchSize(10));

        static::assertCount(1, iterator_to_array(ExecutedPipelines::of($source, $steps, flow_context(config()))));
    }

    public function test_that_rows_are_not_lost(): void
    {
        $source = from_all(from_array([
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
        $steps = [];
        $steps[] = new BatchingProcessor(new BatchSize(7));

        static::assertEquals(
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
            array_map(
                static fn(Rows $r) => $r->toArray(),
                iterator_to_array(ExecutedPipelines::of($source, $steps, flow_context(config()))),
            ),
        );
    }

    public function test_using_bigger_batch_size_than_total_number_of_rows(): void
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
        );
        $steps = [];
        $steps[] = new BatchingProcessor(new BatchSize(11));

        static::assertCount(1, iterator_to_array(ExecutedPipelines::of($source, $steps, flow_context(config()))));
    }

    public function test_using_smaller_batch_size_than_total_number_of_rows(): void
    {
        $source = from_all(from_array([
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
        $steps = [];
        $steps[] = new BatchingProcessor(new BatchSize(5));

        static::assertCount(2, iterator_to_array(ExecutedPipelines::of($source, $steps, flow_context(config()))));
    }
}
