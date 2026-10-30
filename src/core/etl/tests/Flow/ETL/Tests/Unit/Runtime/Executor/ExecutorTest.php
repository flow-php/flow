<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use ArrayObject;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Operators\Transformer\RenameColumnTransformer;
use Flow\ETL\Operators\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node\Batch;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Transform;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Physical\PhysicalPlan;
use Flow\ETL\Planning\Physical\Pipeline;
use Flow\ETL\Planning\Physical\PipelineSteps;
use Flow\ETL\Runtime\Executor\Executor;
use Flow\ETL\Runtime\Executor\SourceCounts;
use Flow\ETL\Tests\Context\ExecutedPlan;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\PlanDrainingTransformer;
use Flow\ETL\Tests\Double\RecordingExtractor;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\Double\RecordingLauncher;
use Flow\ETL\Tests\Double\RecordingRule;
use Flow\ETL\Tests\Double\RecordingSplittableExtractor;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\Double\SpyColumn;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\Double\StubLauncher;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;
use Flow\ETL\Tests\Mother\PipelinesMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use RuntimeException;

use function array_filter;
use function array_keys;
use function array_map;
use function array_sum;
use function array_values;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class ExecutorTest extends FlowTestCase
{
    public function test_consumed_pipelines_are_skipped_and_rest_replaces_the_last(): void
    {
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(4)]);
        $processor = GroupByContext::processor(
            new GroupBy(refs('id'), new Aggregators(count(ref('id')))),
            NodeMother::context(),
            schema(int_schema('id')),
        );
        $limit = new LimitTransformer(1);
        $spy = new SpyTransformer();
        $rows = iterator_to_array((new Executor())->executePlan(
            PipelinesMother::plan([$processor, $limit, $spy], $extractor),
            NodeMother::context(
                config_builder()
                    ->launcher(new StubLauncher(new PipelineSteps(), new PipelineSteps([$limit, $spy]), 2))
                    ->build(),
            ),
        ));

        static::assertSame(1, array_sum(array_map(static fn(Rows $batch): int => $batch->count(), $rows)));
        static::assertSame(1, $spy->seen);
    }

    public function test_merge_concatenates_the_batches_once(): void
    {
        $calls = new ArrayObject();
        $schema = schema(int_schema('id'));

        static::assertSame(
            [1, 2, 3, 4, 5],
            (new Executor())
                ->merge(
                    (static function () use ($schema, $calls) {
                        foreach ([[1], [2], [3], [4], [5]] as $ids) {
                            yield Rows::fromColumns(
                                $schema,
                                ['id' => new SpyColumn(
                                    array_to_rows([['id' => $ids[0]]], $schema)->column('id'),
                                    $calls,
                                )],
                                1,
                            );
                        }

                        return PhysicalPlanMother::reading(from_array([], $schema));
                    })(),
                    NodeMother::context()->backend(),
                )
                ->reduceToArray('id'),
        );
        static::assertSame(
            ['concat'],
            array_values(array_filter($calls->getArrayCopy(), static fn(string $call): bool => $call === 'concat')),
        );
    }

    public function test_merge_of_empty_batches_keeps_the_last_batch(): void
    {
        static::assertSame(
            ['other'],
            array_keys(
                (new Executor())
                    ->merge(
                        (static function () {
                            yield array_to_rows([], schema(int_schema('id')));
                            yield array_to_rows([], schema(int_schema('other')));

                            return PhysicalPlanMother::reading(from_array([], schema(int_schema('other'))));
                        })(),
                        NodeMother::context()->backend(),
                    )
                    ->schema()
                    ->definitions(),
            ),
        );
    }

    public function test_a_single_pipeline_runs_its_steps_in_order(): void
    {
        $loader = new SpyLoader();

        $batches = iterator_to_array((new Executor())->executePipeline(
            new Pipeline(
                0,
                new Read(from_rows(RowsMother::sequentialIds(3))),
                new PipelineSteps([
                    new RenameColumnTransformer('id', 'a'),
                    new BatchingProcessor(new BatchSize(2)),
                    new RenameColumnTransformer('a', 'b'),
                    $loader,
                ]),
            ),
            NodeMother::context(),
        ));

        static::assertSame([[['b' => 1], ['b' => 2]], [['b' => 3]]], [$batches[0]->toArray(), $batches[1]->toArray()]);
        static::assertSame([2, 1], $loader->loadedRowCounts());
    }

    public function test_an_input_chain_runs_pipeline_by_pipeline(): void
    {
        $context = NodeMother::context();

        $batches = iterator_to_array((new Executor())->executePipeline(
            new Pipeline(
                1,
                new Pipeline(
                    0,
                    new Read(from_rows(RowsMother::sequentialIds(5))),
                    new PipelineSteps([new RenameColumnTransformer('id', 'a')], new CollectingProcessor()),
                ),
                new PipelineSteps([
                    new BatchingProcessor(new BatchSize(2)),
                    new RenameColumnTransformer('a', 'b'),
                    new LimitTransformer(3),
                ]),
            ),
            $context,
        ));

        static::assertCount(2, $batches);
        static::assertSame([['b' => 1], ['b' => 2]], $batches[0]->toArray());
        static::assertSame([['b' => 3]], $batches[1]->toArray());
    }

    public function test_stop_reaches_the_source_across_a_cut(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(10));
        $extractor->withBatchSize(1);
        $seenUpstream = new SpyLoader();
        $context = NodeMother::context();

        static::assertCount(
            1,
            iterator_to_array((new Executor())->executePipeline(
                new Pipeline(
                    1,
                    new Pipeline(
                        0,
                        new Read($extractor),
                        new PipelineSteps([$seenUpstream, new BatchingProcessor(new BatchSize(1))]),
                    ),
                    new PipelineSteps([new LimitTransformer(1)]),
                ),
                $context,
            )),
        );
        static::assertSame(1, $extractor->batchesYielded);
        static::assertSame(1, $seenUpstream->loadsCount);
    }

    public function test_every_stage_loads_on_the_runs_backend(): void
    {
        $context = NodeMother::context(config());
        $upstreamLoader = new SpyLoader();
        $downstreamLoader = new SpyLoader();

        iterator_to_array((new Executor())->executePipeline(
            new Pipeline(
                1,
                new Pipeline(
                    0,
                    new Read(from_rows(RowsMother::sequentialIds(1))),
                    new PipelineSteps([$upstreamLoader], new CollectingProcessor()),
                ),
                new PipelineSteps([$downstreamLoader]),
            ),
            $context,
        ));

        static::assertSame([$context->backend()], $upstreamLoader->backends);
        static::assertSame([$context->backend()], $downstreamLoader->backends);
    }

    /**
     * @return Generator<string, array{Extractor}>
     */
    public static function sources_building_on_the_php_backend(): Generator
    {
        $rows = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')), new PhpBackend());

        yield 'a file source' => [new RecordingFileExtractor(schema(int_schema('id')), $rows)];
        yield 'any other source' => [from_rows($rows)];
    }

    #[DataProvider('sources_building_on_the_php_backend')]
    #[RequiresPhpExtension('flow_php')]
    public function test_every_source_yields_its_batches_on_the_pipelines_backend(Extractor $source): void
    {
        $batches = iterator_to_array(
            (new Executor())->executePipeline(
                new Pipeline(0, new Read($source), new PipelineSteps()),
                NodeMother::context(config_builder()->backend(new RustBackend())->build()),
            ),
            false,
        );

        static::assertSame([['id' => 1], ['id' => 2]], $batches[0]->toArray());
        static::assertSame('Flow\ETL\Values\Column\RustColumn', $batches[0]->column('id')::class);
    }

    public function test_the_source_is_extracted_on_the_runs_backend(): void
    {
        $context = NodeMother::context(config());
        $extractor = new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(1));

        iterator_to_array((new Executor())->executePipeline(
            new Pipeline(
                1,
                new Pipeline(0, new Read($extractor), new PipelineSteps([], new CollectingProcessor())),
                new PipelineSteps(),
            ),
            $context,
        ));

        static::assertSame([$context->backend()], $extractor->backends);
    }

    public function test_a_file_source_receives_the_pipelines_criteria(): void
    {
        $extractor = new RecordingFileExtractor(schema(int_schema('id')), RowsMother::sequentialIds(1));
        $criteria = new Criteria(7, pathFilter: new RejectingFilter());

        iterator_to_array((new Executor())->executePipeline(
            new Pipeline(0, new Read($extractor, $criteria), new PipelineSteps()),
            NodeMother::context(),
        ));

        static::assertSame([$criteria], $extractor->criteria);
    }

    public function test_a_source_that_lists_no_files_receives_the_pipelines_limit(): void
    {
        $extractor = new RecordingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(1));

        iterator_to_array((new Executor())->executePipeline(
            new Pipeline(0, new Read($extractor, new Criteria(7)), new PipelineSteps()),
            NodeMother::context(),
        ));

        static::assertSame([7], $extractor->limits);
    }

    public function test_a_pipeline_run_opens_no_dataframe_span_for_any_stage(): void
    {
        $upstream = new MemoryTelemetryContext();
        $own = new MemoryTelemetryContext();

        iterator_to_array((new Executor())->executePipeline(
            new Pipeline(
                1,
                new Pipeline(
                    0,
                    new Read(from_rows(RowsMother::sequentialIds(1))),
                    new PipelineSteps([], new CollectingProcessor()),
                ),
                new PipelineSteps(),
            ),
            $own->flowContext,
        ));

        static::assertSame([], $upstream->spans->startedSpans());
        static::assertSame([], $own->spans->startedSpans());
    }

    public function test_execute_plan_runs_the_plans_root_pipeline(): void
    {
        $batches = iterator_to_array((new Executor())->executePlan(new PhysicalPlan(PipelinesMother::root(
            from_rows(RowsMother::sequentialIds(3)),
            [new BatchingProcessor(new BatchSize(2)), new RenameColumnTransformer('id', 'b')],
        ), schema(int_schema('b'))), NodeMother::context()));

        static::assertCount(2, $batches);
        static::assertSame([['b' => 1], ['b' => 2]], $batches[0]->toArray());
    }

    public function test_fetch_merges_every_batch_into_one_rows(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2], ['id' => 3]],
            (new Executor())
                ->fetch(
                    NodeMother::plan(
                        new Batch(NodeMother::read(from_rows(RowsMother::sequentialIds(3))), new BatchSize(1)),
                    ),
                    NodeMother::context(),
                )
                ->toArray(),
        );
    }

    public function test_fetch_of_an_empty_plan_returns_rows_with_the_output_schema(): void
    {
        $rows = (new Executor())->fetch(
            NodeMother::plan(NodeMother::read(from_rows(rows(schema(int_schema('id')))))),
            NodeMother::context(),
        );

        static::assertSame(0, $rows->count());
        static::assertEquals(schema(int_schema('id')), $rows->schema());
    }

    public function test_merge_merges_every_batch_into_one_rows(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            (new Executor())
                ->merge(
                    (static function () {
                        yield RowsMother::sequentialIds(1);
                        yield array_to_rows([['id' => 2]], schema(int_schema('id')));

                        return new PhysicalPlan(
                            PipelinesMother::root(from_rows(rows(schema(int_schema('id')))), []),
                            schema(int_schema('id')),
                            null,
                        );
                    })(),
                    NodeMother::context()->backend(),
                )
                ->toArray(),
        );
    }

    public function test_merge_of_no_batches_takes_the_schema_of_the_plan_the_generator_returns(): void
    {
        $rows = (new Executor())->merge(
            (static function () {
                yield from [];

                return new PhysicalPlan(
                    PipelinesMother::root(from_rows(rows(schema(int_schema('id')))), []),
                    schema(int_schema('id')),
                    null,
                );
            })(),
            NodeMother::context()->backend(),
        );

        static::assertSame(0, $rows->count());
        static::assertEquals(schema(int_schema('id')), $rows->schema());
    }

    public function test_merge_of_no_batches_from_a_refused_plan_returns_rows_with_an_empty_schema(): void
    {
        $rows = (new Executor())->merge(
            (static function () {
                yield from [];

                return new PhysicalPlan(
                    PipelinesMother::root(from_rows(rows(schema(int_schema('id')))), []),
                    null,
                    SchemaNotDerivableException::extractor('x'),
                );
            })(),
            NodeMother::context()->backend(),
        );

        static::assertSame(0, $rows->count());
        static::assertEquals(schema(), $rows->schema());
    }

    public function test_execute_opens_and_closes_the_plans_dataframe_span(): void
    {
        $telemetry = new MemoryTelemetryContext();
        iterator_to_array((new Executor())->execute(
            NodeMother::plan(NodeMother::read(from_rows(RowsMother::sequentialIds(1)))),
            $telemetry->flowContext,
        ));

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
    }

    public function test_a_failing_frame_closes_its_span_as_failed_and_rethrows(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $failure = new RuntimeException('right side exploded');
        $plan = NodeMother::plan(
            new Transform(NodeMother::read(from_rows(RowsMother::sequentialIds(1))), new ThrowingTransformer($failure)),
        );

        try {
            (new Executor())->fetch($plan, $telemetry->flowContext);

            static::fail('Expected the frame failure to be rethrown.');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }

    public function test_a_planning_failure_closes_the_dataframe_span_as_failed_once(): void
    {
        $telemetry = new MemoryTelemetryContext();

        try {
            iterator_to_array((new Executor())->execute(
                NodeMother::plan(NodeMother::select(NodeMother::read(), 'nope')),
                $telemetry->flowContext,
            ));

            static::fail('Expected the bind failure to be rethrown.');
        } catch (SchemaDefinitionNotFoundException $e) {
            static::assertSame('Schema definition for column "nope" not found.', $e->getMessage());
        }

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
        static::assertCount(1, $telemetry->logs->entriesContaining('Data frame processing started'));
        static::assertCount(1, $telemetry->logs->entriesContaining('Data frame processing failed'));
    }

    public function test_a_plan_with_two_breakers_returns_the_rows_of_one_pass(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 2], ['id' => 3]],
            df()
                ->read(from_array([['id' => 3], ['id' => 1], ['id' => 2]]))
                ->sortBy(ref('id'))
                ->collect()
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_stop_from_a_limit_after_a_join_still_stops_the_left_source(): void
    {
        $left = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(10)))->withBatchSize(1);

        static::assertSame(
            [['id' => 1, 'r_id' => 1]],
            df()
                ->read($left)
                ->join(df()->read(from_array([['id' => 1], ['id' => 2]])), join_on(['id' => 'id'], 'r_'))
                ->limit(1)
                ->fetch()
                ->toArray(),
        );
        static::assertLessThan(10, $left->batchesYielded);
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
                array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))),
                array_to_rows([['id' => 3], ['id' => 4]], schema(int_schema('id'))),
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

        static::assertSame(
            [['id' => 1]],
            iterator_to_array(ExecutedPlan::of(
                NodeMother::plan(NodeMother::read()),
                NodeMother::context(
                    config_builder()->optimizer(new Optimizer(new RecordingRule('plan', $log)))->build(),
                ),
            ))[0]->toArray(),
        );
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
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => 2]], schema(int_schema('id'))),
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

    public function test_every_source_is_adopted_once_per_column_and_batch(): void
    {
        $backend = new SpyBackend();
        $context = NodeMother::context(config_builder()->backend($backend)->build());

        iterator_to_array((new Executor())->executePipeline(
            PipelinesMother::root(
                new RecordingFileExtractor(schema(int_schema('id')), RowsMother::sequentialIds(2)),
                [],
            ),
            $context,
        ));

        static::assertSame(1, $backend->adopts());

        iterator_to_array((new Executor())->executePipeline(
            PipelinesMother::root(new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(2)), []),
            $context,
        ));

        static::assertSame(2, $backend->adopts());
    }

    public function test_a_parallel_source_is_read_through_the_configs_launcher(): void
    {
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(1)]);
        $launcher = new RecordingLauncher(new StubLauncher());
        $criteria = new Criteria(3);
        $context = NodeMother::context(config_builder()->launcher($launcher)->build());
        $plan = new PhysicalPlan(
            new Pipeline(0, new Read($extractor, $criteria), new PipelineSteps()),
            schema(int_schema('id')),
            null,
        );

        iterator_to_array((new Executor())->executePlan($plan, $context));

        static::assertCount(1, $launcher->reads);
        static::assertSame($plan, $launcher->reads[0][0]);
        static::assertSame($extractor, $launcher->reads[0][1]);
        static::assertSame($criteria, $launcher->reads[0][2]);
        static::assertSame($context, $launcher->reads[0][3]);
        static::assertSame([3], $extractor->limits);
    }

    public function test_without_a_launcher_a_parallel_source_runs_here(): void
    {
        $context = NodeMother::context(config_builder()->build());
        $plan = $context->config->planner()->plan(NodeMother::plan(new Transform(NodeMother::read(
            new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(4)]),
        ), new ScalarFunctionFilterTransformer(ref('id')->greaterThan(lit(2))))));

        static::assertSame(
            [[3, 4]],
            array_map(static fn(Rows $batch): array => $batch->reduceToArray(
                'id',
            ), iterator_to_array((new Executor())->executePlan($plan, $context))),
        );
    }

    public function test_a_launcher_that_launches_nothing_leaves_a_parallel_source_to_the_executor(): void
    {
        $launcher = new RecordingLauncher();
        $context = NodeMother::context(config_builder()->launcher($launcher)->build());
        $plan = $context->config->planner()->plan(NodeMother::plan(new Transform(NodeMother::read(
            new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(4)]),
        ), new ScalarFunctionFilterTransformer(ref('id')->greaterThan(lit(2))))));

        static::assertSame([], $launcher->reads);

        $rows = iterator_to_array((new Executor())->executePlan($plan, $context));

        static::assertCount(1, $launcher->reads);
        static::assertSame([[3, 4]], array_map(static fn(Rows $batch): array => $batch->reduceToArray('id'), $rows));
    }

    public function test_a_source_that_cannot_be_split_never_reaches_the_launcher(): void
    {
        $launcher = new RecordingLauncher(new StubLauncher());
        $context = NodeMother::context(config_builder()->launcher($launcher)->build());

        iterator_to_array((new Executor())->executePlan(
            $context
                ->config
                ->planner()
                ->plan(NodeMother::plan(NodeMother::read(from_rows(RowsMother::sequentialIds(2))))),
            $context,
        ));

        static::assertSame([], $launcher->reads);
    }

    public function test_the_launcher_gets_the_sources_and_the_parent_resumes_after_the_launch(): void
    {
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(4)]);
        $filter = new ScalarFunctionFilterTransformer(ref('id')->greaterThan(lit(2)));
        $sources = new SourceCounts();
        $launcher = new RecordingLauncher(new StubLauncher(new PipelineSteps([$filter])));

        $rows = iterator_to_array((new Executor())->executePlan(
            new PhysicalPlan(
                new Pipeline(0, new Read($extractor), new PipelineSteps([$filter])),
                schema(int_schema('id')),
                null,
            ),
            NodeMother::context(config_builder()->launcher($launcher)->build()),
            $sources,
        ));

        static::assertSame($sources, $launcher->reads[0][4]);
        static::assertSame([[3, 4]], array_map(static fn(Rows $batch): array => $batch->reduceToArray('id'), $rows));
        static::assertSame(4, $sources->statistics()[0]->rows);
    }

    public function test_the_rest_replaces_the_leaf_pipelines_steps(): void
    {
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(4)]);
        $filter = new ScalarFunctionFilterTransformer(ref('id')->greaterThan(lit(2)));
        $spy = new SpyTransformer();
        $rows = iterator_to_array((new Executor())->executePlan(
            PipelinesMother::plan([$filter, $spy], $extractor),
            NodeMother::context(
                config_builder()
                    ->launcher(new StubLauncher(new PipelineSteps([$filter]), new PipelineSteps([$spy])))
                    ->build(),
            ),
        ));

        static::assertSame([[3, 4]], array_map(static fn(Rows $batch): array => $batch->reduceToArray('id'), $rows));
        static::assertSame(2, $spy->seen);
    }

    public function test_only_the_pipeline_that_reads_the_source_counts_its_rows(): void
    {
        $sources = new SourceCounts();
        $context = NodeMother::context();

        iterator_to_array((new Executor())->execute(
            new LogicalPlan(new Result(NodeMother::limit(
                NodeMother::sort(NodeMother::read(from_array([
                    ['id' => 1],
                    ['id' => 2],
                ], schema(int_schema('id'))))),
                5,
            ))),
            $context,
            $sources,
        ));

        static::assertCount(1, $sources->statistics());
        static::assertSame(2, $sources->statistics()[0]->rows);
    }
}
