<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use ArrayObject;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Executor;
use Flow\ETL\Executor\Described;
use Flow\ETL\Executor\Pipeline;
use Flow\ETL\Executor\Raw;
use Flow\ETL\Executor\Segments;
use Flow\ETL\Optimizer;
use Flow\ETL\Plan\Node\Transform;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\ExecutedPlan;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\PlanDrainingTransformer;
use Flow\ETL\Tests\Double\RecordingExtractor;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\Double\RecordingRule;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use RuntimeException;

use function array_map;
use function array_sum;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class ExecutorTest extends FlowTestCase
{
    public function test_a_single_pipeline_runs_its_segments_in_order(): void
    {
        $segments = new Segments(from_rows(RowsMother::sequentialIds(3)));
        $segments->add(new RenameEntryTransformer('id', 'a'));
        $segments->add(new BatchingProcessor(2));
        $segments->add(new RenameEntryTransformer('a', 'b'));
        $segments->add($loader = new SpyLoader());

        $batches = iterator_to_array((new Executor())->executePipeline(
            new Pipeline(0, $segments, NodeMother::context()),
        ));

        static::assertSame([[['b' => 1], ['b' => 2]], [['b' => 3]]], [$batches[0]->toArray(), $batches[1]->toArray()]);
        static::assertSame([2, 1], $loader->loadedRowCounts());
    }

    public function test_an_input_chain_is_flattened_into_one_generator_chain(): void
    {
        $upstream = new Segments(from_rows(RowsMother::sequentialIds(5)));
        $upstream->add(new RenameEntryTransformer('id', 'a'));
        $upstream->add(new BatchingProcessor(2));
        $downstream = new Segments();
        $downstream->add(new RenameEntryTransformer('a', 'b'));
        $downstream->add(new LimitTransformer(3));
        $context = NodeMother::context();

        $batches = iterator_to_array((new Executor())->executePipeline(
            new Pipeline(1, $downstream, $context, new Pipeline(0, $upstream, $context)),
        ));

        static::assertCount(2, $batches);
        static::assertSame([['b' => 1], ['b' => 2]], $batches[0]->toArray());
        static::assertSame([['b' => 3]], $batches[1]->toArray());
    }

    public function test_stop_reaches_the_source_across_a_cut(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(10));
        $extractor->withBatchSize(1);
        $upstream = new Segments($extractor);
        $upstream->add($seenUpstream = new SpyLoader());
        $upstream->add(new BatchingProcessor(1));
        $downstream = new Segments();
        $downstream->add(new LimitTransformer(1));
        $context = NodeMother::context();

        $batches = iterator_to_array((new Executor())->executePipeline(
            new Pipeline(1, $downstream, $context, new Pipeline(0, $upstream, $context)),
        ));

        static::assertCount(1, $batches);
        static::assertSame(1, $extractor->batchesYielded);
        static::assertSame(1, $seenUpstream->loadsCount);
    }

    public function test_each_stage_runs_under_its_own_flow_context(): void
    {
        $upstreamContext = NodeMother::context(config());
        $downstreamContext = NodeMother::context(config());
        $upstream = new Segments(from_rows(RowsMother::sequentialIds(1)));
        $upstream->add($upstreamLoader = new SpyLoader());
        $upstream->add(new BatchingProcessor(1));
        $downstream = new Segments();
        $downstream->add($downstreamLoader = new SpyLoader());

        iterator_to_array((new Executor())->executePipeline(
            new Pipeline(1, $downstream, $downstreamContext, new Pipeline(0, $upstream, $upstreamContext)),
        ));

        static::assertSame([$upstreamContext], $upstreamLoader->contexts);
        static::assertSame([$downstreamContext], $downstreamLoader->contexts);
    }

    public function test_the_source_is_extracted_under_the_leaf_pipelines_context(): void
    {
        $leafContext = NodeMother::context(config());
        $extractor = new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(1));
        $upstream = new Segments($extractor);
        $upstream->add(new BatchingProcessor(1));

        iterator_to_array((new Executor())->executePipeline(
            new Pipeline(1, new Segments(), NodeMother::context(config()), new Pipeline(0, $upstream, $leafContext)),
        ));

        static::assertSame([$leafContext], $extractor->contexts);
    }

    public function test_a_pipeline_without_a_source_throws(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('pipeline #7 has no source extractor');

        iterator_to_array((new Executor())->executePipeline(new Pipeline(7, new Segments(), NodeMother::context())));
    }

    public function test_a_file_source_receives_the_pipelines_limit_and_path_filter(): void
    {
        $extractor = new RecordingFileExtractor(schema(int_schema('id')), RowsMother::sequentialIds(1));
        $filter = new RejectingFilter();

        iterator_to_array((new Executor())->executePipeline(
            new Pipeline(0, new Segments($extractor), NodeMother::context(), limit: 7, pathFilter: $filter),
        ));

        static::assertSame([7], $extractor->limits);
        static::assertSame([$filter], $extractor->pathFilters);
    }

    public function test_a_source_that_lists_no_files_receives_the_pipelines_limit(): void
    {
        $extractor = new RecordingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(1));

        iterator_to_array((new Executor())->executePipeline(
            new Pipeline(0, new Segments($extractor), NodeMother::context(), limit: 7),
        ));

        static::assertSame([7], $extractor->limits);
    }

    public function test_a_pipeline_run_opens_no_dataframe_span_for_any_stage(): void
    {
        $upstream = new MemoryTelemetryContext();
        $own = new MemoryTelemetryContext();
        $segments = new Segments(from_rows(RowsMother::sequentialIds(1)));
        $segments->add(new BatchingProcessor(1));

        iterator_to_array((new Executor())->executePipeline(
            new Pipeline(1, new Segments(), $own->flowContext, new Pipeline(0, $segments, $upstream->flowContext)),
        ));

        static::assertSame([], $upstream->spans->startedSpans());
        static::assertSame([], $own->spans->startedSpans());
    }

    public function test_execute_runs_the_plans_root_pipeline(): void
    {
        $segments = new Segments(from_rows(RowsMother::sequentialIds(3)));
        $segments->add(new BatchingProcessor(2));
        $segments->add(new RenameEntryTransformer('id', 'b'));

        $batches = iterator_to_array((new Executor())->execute(
            new Described(new Pipeline(0, $segments, NodeMother::context()), schema(int_schema('b'))),
        ));

        static::assertCount(2, $batches);
        static::assertSame([['b' => 1], ['b' => 2]], $batches[0]->toArray());
    }

    public function test_fetch_merges_every_batch_into_one_rows(): void
    {
        $segments = new Segments(from_rows(RowsMother::sequentialIds(3)));
        $segments->add(new BatchingProcessor(1));

        $rows = (new Executor())->fetch(
            new Described(new Pipeline(0, $segments, NodeMother::context()), schema(int_schema('id'))),
        );

        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], $rows->toArray());
    }

    public function test_fetch_of_an_empty_pipeline_returns_rows_with_the_output_schema(): void
    {
        $rows = (new Executor())->fetch(
            new Described(
                new Pipeline(0, new Segments(from_rows(rows(schema(int_schema('id'))))), NodeMother::context()),
                schema(int_schema('id')),
            ),
        );

        static::assertSame(0, $rows->count());
        static::assertEquals(schema(int_schema('id')), $rows->schema());
    }

    public function test_fetch_of_an_empty_refused_pipeline_returns_rows_with_an_empty_schema(): void
    {
        $rows = (new Executor())->fetch(
            new Raw(
                new Pipeline(0, new Segments(from_rows(rows(schema(int_schema('id'))))), NodeMother::context()),
                SchemaNotDerivableException::extractor('x'),
            ),
        );

        static::assertSame(0, $rows->count());
        static::assertEquals(schema(), $rows->schema());
    }

    public function test_merge_merges_every_batch_into_one_rows(): void
    {
        $plan = new Described(
            new Pipeline(0, new Segments(from_rows(rows(schema(int_schema('id'))))), NodeMother::context()),
            schema(int_schema('id')),
        );
        $batches = (static function () {
            yield RowsMother::sequentialIds(1);
            yield rows(schema(int_schema('id')), row(['id' => 2]));
        })();

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            (new Executor())
                ->merge($batches, $plan)
                ->toArray(),
        );
    }

    public function test_merge_of_no_batches_returns_rows_with_the_plans_schema(): void
    {
        $plan = new Described(
            new Pipeline(0, new Segments(from_rows(rows(schema(int_schema('id'))))), NodeMother::context()),
            schema(int_schema('id')),
        );

        $rows = (new Executor())->merge((static fn() => yield from [])(), $plan);

        static::assertSame(0, $rows->count());
        static::assertEquals(schema(int_schema('id')), $rows->schema());
    }

    public function test_merge_of_no_batches_from_a_refused_plan_returns_rows_with_an_empty_schema(): void
    {
        $plan = new Raw(
            new Pipeline(0, new Segments(from_rows(rows(schema(int_schema('id'))))), NodeMother::context()),
            SchemaNotDerivableException::extractor('x'),
        );

        $rows = (new Executor())->merge((static fn() => yield from [])(), $plan);

        static::assertSame(0, $rows->count());
        static::assertEquals(schema(), $rows->schema());
    }

    public function test_execute_opens_and_closes_the_plans_dataframe_span(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $segments = new Segments(from_rows(RowsMother::sequentialIds(1)));

        iterator_to_array((new Executor())->execute(
            new Described(new Pipeline(0, $segments, $telemetry->flowContext), schema(int_schema('id'))),
        ));

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
    }

    public function test_a_failing_frame_closes_its_span_as_failed_and_rethrows(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $failure = new RuntimeException('right side exploded');
        $segments = new Segments(from_rows(RowsMother::sequentialIds(1)));
        $segments->add(new ThrowingTransformer($failure));
        $plan = new Described(new Pipeline(0, $segments, $telemetry->flowContext), schema(int_schema('id')));

        try {
            (new Executor())->fetch($plan);

            static::fail('Expected the frame failure to be rethrown.');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }

    public function test_execute_yields_the_batches_of_the_planned_pipeline(): void
    {
        $batches = iterator_to_array(ExecutedPlan::of(
            NodeMother::plan(NodeMother::limit(NodeMother::read(from_array([['id' => 1], ['id' => 2]])), 1)),
            NodeMother::context(),
        ));

        static::assertCount(1, $batches);
        static::assertSame([['id' => 1]], $batches[0]->toArray());
    }

    public function test_a_second_run_of_the_same_logical_plan_gets_fresh_steps(): void
    {
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::read(from_array([['id' => 1], ['id' => 2]])), 1));
        $context = NodeMother::context();

        iterator_to_array(ExecutedPlan::of($plan, $context));

        static::assertSame([['id' => 1]], iterator_to_array(ExecutedPlan::of($plan, $context))[0]->toArray());
    }

    public function test_every_execution_plans_its_own(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();
        $context = NodeMother::context(
            config_builder()->optimizer(new Optimizer(new RecordingRule('plan', $log)))->build(),
        );
        $plan = NodeMother::plan(NodeMother::read());

        iterator_to_array(ExecutedPlan::of($plan, $context));
        iterator_to_array(ExecutedPlan::of($plan, $context));

        static::assertCount(2, $log);
    }

    public function test_an_abandoned_run_leaves_no_consumed_step_for_the_next_run(): void
    {
        $plan = NodeMother::plan(NodeMother::limit(
            NodeMother::read(from_rows(
                rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
                rows(schema(int_schema('id')), row(['id' => 3]), row(['id' => 4])),
            )),
            3,
        ));
        $context = NodeMother::context();

        // the reference keeps the generator parked, so its steps stay consumed
        $parked = ExecutedPlan::of($plan, $context);
        $parked->current();

        static::assertSame(
            3,
            array_sum(array_map(
                static fn(Rows $rows): int => $rows->count(),
                iterator_to_array(ExecutedPlan::of($plan, $context), false),
            )),
        );
    }

    public function test_a_plan_reading_back_from_itself_throws_cyclic_plan_on_run(): void
    {
        $context = NodeMother::context();
        $read = NodeMother::read();

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('Cannot run this plan: it reads from a DataFrame that reads back from it.');

        iterator_to_array(ExecutedPlan::of(
            NodeMother::plan(new Transform($read, new PlanDrainingTransformer(NodeMother::plan($read), $context))),
            $context,
        ));
    }

    public function test_the_reentrancy_guard_is_disarmed_across_the_yield(): void
    {
        $plan = NodeMother::plan(NodeMother::read(from_array([['id' => 1], ['id' => 2], ['id' => 3]])));
        $context = NodeMother::context();

        // the reference keeps the generator parked across the read below - inlining it lets PHP
        // destroy the generator, which clears the flag and makes this test vacuous
        $parked = ExecutedPlan::of($plan, $context);
        $parked->current();

        static::assertCount(1, iterator_to_array(ExecutedPlan::of($plan, $context)));
    }

    public function test_a_plan_is_planned_with_the_planner_of_the_contexts_config(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();
        $context = NodeMother::context(
            config_builder()->optimizer(new Optimizer(new RecordingRule('plan', $log)))->build(),
        );

        $batches = iterator_to_array(ExecutedPlan::of(NodeMother::plan(NodeMother::read()), $context));

        static::assertSame([['id' => 1]], $batches[0]->toArray());
        static::assertSame(['plan'], $log->getArrayCopy());
    }

    public function test_a_drained_run_reports_one_balanced_dataframe_span(): void
    {
        $telemetry = new MemoryTelemetryContext();

        iterator_to_array(ExecutedPlan::of(NodeMother::plan(NodeMother::read()), $telemetry->flowContext));

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertNotTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }

    public function test_an_executor_failure_reports_the_span_as_failed_and_rethrows(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $failure = new RuntimeException('source exploded');

        try {
            iterator_to_array(ExecutedPlan::of(
                NodeMother::plan(new Transform(NodeMother::read(), new ThrowingTransformer($failure))),
                $telemetry->flowContext,
            ));

            static::fail('Expected the executor failure to be rethrown.');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }

    public function test_an_abandoned_run_closes_its_span(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $generator = ExecutedPlan::of(
            NodeMother::plan(NodeMother::read(from_rows(
                rows(schema(int_schema('id')), row(['id' => 1])),
                rows(schema(int_schema('id')), row(['id' => 2])),
            ))),
            $telemetry->flowContext,
        );

        $generator->current();
        unset($generator);

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertNotTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }

    public function test_another_plan_run_during_an_armed_outer_run_does_not_trip_the_guard(): void
    {
        $other = new PlanDrainingTransformer(NodeMother::plan(NodeMother::read()), NodeMother::context());

        iterator_to_array(ExecutedPlan::of(
            NodeMother::plan(new Transform(NodeMother::read(), $other)),
            NodeMother::context(),
        ));

        static::assertCount(1, $other->drained);
        static::assertSame([['id' => 1]], $other->drained[0]->toArray());
    }
}
