<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\DataDependentSchemaException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\GroupBy;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Tests\Double\ReadsBackOnSecondBatch;
use Flow\ETL\Tests\Double\StaticDataFrameFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class PipelineTest extends FlowTestCase
{
    public function test_a_join_each_cycle_is_refused_when_run(): void
    {
        $frame = df()->read(from_array([['id' => 1, 'x' => 'p']]));
        $frame->joinEach(new StaticDataFrameFactory(df()->read(from_data_frame($frame))), join_on([
            'id' => 'id',
        ], 'r_'));

        try {
            $frame->schema();

            static::fail('Expected joinEach() to refuse to describe its output.');
        } catch (DataDependentSchemaException $e) {
            static::assertStringContainsString('JoinEachRowsTransformer', $e->getMessage());
        }

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('Cannot run this plan: it reads from a DataFrame that reads back from it.');

        $frame->fetch();
    }

    public function test_a_plan_can_be_run_twice(): void
    {
        $frame = df()->read(from_array([['id' => 1], ['id' => 2]]));

        static::assertSame(2, $frame->count());
        static::assertSame([['id' => 1], ['id' => 2]], $frame->fetch()->toArray());
    }

    public function test_a_cycle_closed_on_a_later_batch_is_refused_when_run(): void
    {
        $frame = df()
            ->read(from_rows(
                rows(schema(int_schema('id')), row(['id' => 1])),
                rows(schema(int_schema('id')), row(['id' => 2])),
            ))
            ->transform($transformer = new ReadsBackOnSecondBatch());

        $transformer->readBackFrom($frame);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('Cannot run this plan: it reads from a DataFrame that reads back from it.');

        $frame->fetch();
    }

    public function test_a_plan_read_again_while_an_earlier_generator_is_parked_is_not_a_cycle(): void
    {
        $frame = df()->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]));

        // the reference keeps the generator parked across the read below - inlining it lets PHP
        // destroy the generator, which clears the flag and makes this test vacuous
        $parked = $frame->get();
        $parked->current();

        static::assertSame(3, $frame->fetch()->count());
    }

    public function test_a_plan_that_reads_back_from_itself_is_refused_when_described(): void
    {
        $frame = df()->read(from_array([['id' => 1, 'x' => 'p']]));
        $frame->join(df()->read(from_data_frame($frame)), join_on(['id' => 'id'], 'r_'));

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'Cannot describe this plan: it reads from a DataFrame that reads back from it. A DataFrame is '
            . 'mutable, so join(), select() and withEntry() can add that edge after both frames exist. Break '
            . 'the cycle by reading the nested frame from its own source.',
        );

        $frame->schema();
    }

    public function test_fetching_a_plan_that_reads_back_from_itself_is_refused_at_bind(): void
    {
        $frame = df()->read(from_array([['id' => 1, 'x' => 'p']]));
        $frame->join(df()->read(from_data_frame($frame)), join_on(['id' => 'id'], 'r_'));

        // this used to segfault
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'Cannot describe this plan: it reads from a DataFrame that reads back from it. A DataFrame is '
            . 'mutable, so join(), select() and withEntry() can add that edge after both frames exist. Break '
            . 'the cycle by reading the nested frame from its own source.',
        );

        $frame->fetch();
    }

    public function test_getting_steps_from_pipeline(): void
    {
        $pipeline = new Pipeline(from_rows(rows(schema())));
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

    public function test_the_same_frame_read_twice_in_one_plan_is_not_a_cycle(): void
    {
        $source = df()->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]));
        $left = df()->read(from_data_frame($source));
        $left->join(df()->read(from_data_frame($source)), join_on(['id' => 'id'], 'r_'));

        static::assertSame(3, $left->fetch()->count());
    }
}
