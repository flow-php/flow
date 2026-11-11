<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use ArrayObject;
use Flow\ETL\API\Transactional;
use Flow\ETL\Foundation\Clock\FakeClock;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\StageProgress;
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
use Flow\ETL\Runtime\ErrorHandler;
use Flow\ETL\Runtime\ErrorHandler\IgnoreError;
use Flow\ETL\Runtime\ErrorHandler\SkipRows;
use Flow\ETL\Runtime\ErrorHandler\ThrowError;
use Flow\ETL\Runtime\Executor\Executor;
use Flow\ETL\Runtime\Executor\RunCounts;
use Flow\ETL\Runtime\Heartbeat\HeartbeatFailure;
use Flow\ETL\Runtime\Heartbeat\Pulse;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Context\ExecutedPlan;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\ClosureThrowingLoader;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\CountingFileLoader;
use Flow\ETL\Tests\Double\DelegatingLauncher;
use Flow\ETL\Tests\Double\EagerFailureProcessor;
use Flow\ETL\Tests\Double\PassThroughProcessor;
use Flow\ETL\Tests\Double\PlanDrainingTransformer;
use Flow\ETL\Tests\Double\RecordingExtractor;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Tests\Double\RecordingLauncher;
use Flow\ETL\Tests\Double\RecordingLoader;
use Flow\ETL\Tests\Double\RecordingRule;
use Flow\ETL\Tests\Double\RecordingSplittableExtractor;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\ReportingAtEndLoader;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\Double\SpyColumn;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\Double\StaticExtractorFactory;
use Flow\ETL\Tests\Double\SteppingClock;
use Flow\ETL\Tests\Double\StubLauncher;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\Double\TracedFailureProcessor;
use Flow\ETL\Tests\Double\TracedFailureTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;
use Flow\ETL\Tests\Mother\PipelinesMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use Flow\Telemetry\Logger\Severity;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use RuntimeException;

use function array_filter;
use function array_key_exists;
use function array_key_last;
use function array_keys;
use function array_map;
use function array_merge;
use function array_pop;
use function array_slice;
use function array_sum;
use function array_values;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\overwrite;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;
use function Flow\ETL\DSL\to_branch;
use function Flow\ETL\DSL\to_memory;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\from_floe;
use function Flow\Floe\DSL\to_floe;
use function in_array;
use function ini_set;
use function iterator_to_array;
use function memory_get_peak_usage;
use function range;
use function sort;
use function str_repeat;
use function usort;

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

    public function test_counts_rows_read_into_the_heartbeat(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array(RowsMother::sequentialIds(30)->toArray())->withBatchSize(10))
            ->fetch();

        static::assertSame([30, [30]], [
            array_slice($recording->pulses(), -1)[0]->rowsRead,
            array_map(
                static fn(StageProgress $read): int => $read->count,
                array_slice(HeartbeatContext::entries($recording, StageName::read), -1)[0],
            ),
        ]);
    }

    public function test_a_spilled_source_reports_its_inference_before_the_first_batch_is_read(): void
    {
        $recording = new RecordingHeartbeat();
        /** @var callable(): Generator<int, array<string, int>> $generator */
        $generator = static function (): Generator {
            for ($id = 1; $id <= 2500; $id++) {
                yield ['id' => $id];
            }
        };

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array($generator(), filesystem: memory_filesystem(), spillRoot: path('memory://tmp')))
            ->fetch();

        $first = $recording->pulses()[0];
        static::assertSame([['infer'], 0], [
            array_map(static fn(StageProgress $stage): string => $stage->name->value, $first->stages),
            $first->rowsRead,
        ]);
        static::assertContains(
            [2500],
            array_map(
                static fn(array $reads): array => array_map(
                    static fn(StageProgress $read): int => $read->count,
                    $reads,
                ),
                HeartbeatContext::entries($recording, StageName::read),
            ),
        );
    }

    public function test_schema_beats_during_inference_and_once_at_its_end(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 0.1))
            ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], range(1, 2500))))
            ->schema();

        $entries = HeartbeatContext::entries($recording, StageName::infer);
        static::assertSame([true, null, true], [
            array_key_exists(1, $entries),
            $entries[0][0]->endedAtSeconds,
            array_slice($entries, -1)[0][0]->endedAtSeconds !== null,
        ]);
    }

    public function test_a_stop_throw_during_inference_comes_out_of_schema(): void
    {
        $lost = new RuntimeException('lease lost');
        $recording = new RecordingHeartbeat(throws: $lost);
        $frame = df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 0.1))
            ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], range(1, 2500))));
        $thrown = [];

        foreach ([1, 2] as $_) {
            try {
                $frame->schema();
            } catch (RuntimeException $exception) {
                $thrown[] = [$exception, array_key_last($recording->pulses())];
            }
        }

        static::assertSame([[$lost, 0], [$lost, 1]], $thrown);
    }

    public function test_the_peak_of_a_run_is_not_the_peak_of_an_earlier_run(): void
    {
        $recording = new RecordingHeartbeat();
        $freed = str_repeat('x', 64 * 1024 * 1024);
        unset($freed);

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1]]))->run();

        static::assertLessThan(memory_get_peak_usage(true), array_slice($recording->pulses(), -1)[0]->peakMemoryBytes);
    }

    public function test_a_write_to_memory_has_a_write_entry_that_counts_its_rows(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))
            ->write(to_memory(new ArrayMemory()))
            ->run();

        $pulses = HeartbeatContext::entries($recording, StageName::write);
        static::assertSame(
            [[StageName::write, 2, null, true]],
            array_map(
                static fn(StageProgress $write): array => [
                    $write->name,
                    $write->count,
                    $write->subject,
                    $write->endedAtSeconds !== null,
                ],
                array_pop($pulses) ?? [],
            ),
        );
    }

    public function test_a_write_entry_is_listed_from_the_first_beat(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))
            ->write(to_memory(new ArrayMemory()))
            ->run();

        static::assertSame(
            [null],
            array_map(
                static fn(StageProgress $write): ?float => $write->startedAtSeconds,
                HeartbeatContext::entries($recording, StageName::write)[0],
            ),
        );
    }

    public function test_a_write_to_files_has_a_write_entry_then_its_commit_entry(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))
            ->write(to_floe(path('memory://out/rows.floe'), filesystem: memory_filesystem()))
            ->run();

        $pulses = $recording->pulses();
        static::assertSame(
            [
                ['write',  2, 'memory://out/'],
                ['commit', 1, 'memory://out/'],
            ],
            array_map(
                static fn(StageProgress $stage): array => [$stage->name->value, $stage->count, $stage->subject],
                array_values(array_filter(
                    array_pop($pulses)->stages ?? [],
                    static fn(StageProgress $stage): bool => in_array(
                        $stage->name,
                        [StageName::write, StageName::commit],
                        true,
                    ),
                )),
            ),
        );
    }

    public function test_a_transaction_of_a_file_and_a_memory_write_has_two_write_entries_and_one_commit_entry(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))
            ->write(
                new Transactional(
                    new RecordingTransaction(),
                    to_floe(path('memory://out/rows.floe'), filesystem: memory_filesystem()),
                    to_memory(new ArrayMemory()),
                ),
            )
            ->run();

        $pulses = $recording->pulses();
        static::assertSame(
            [
                ['write',  2, 'memory://out/'],
                ['commit', 1, 'memory://out/'],
                ['write',  2, null],
            ],
            array_map(
                static fn(StageProgress $stage): array => [$stage->name->value, $stage->count, $stage->subject],
                array_values(array_filter(
                    array_pop($pulses)->stages ?? [],
                    static fn(StageProgress $stage): bool => in_array(
                        $stage->name,
                        [StageName::write, StageName::commit],
                        true,
                    ),
                )),
            ),
        );
    }

    public function test_a_write_in_a_branch_counts_the_rows_of_the_branch(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))
            ->write(to_branch(
                ref('id')->greaterThan(lit(1)),
                to_floe(path('memory://out/rows.floe'), filesystem: memory_filesystem()),
            ))
            ->run();

        $pulses = HeartbeatContext::entries($recording, StageName::write);
        static::assertSame(
            [1],
            array_map(static fn(StageProgress $write): int => $write->count, array_pop($pulses) ?? []),
        );
    }

    public function test_a_write_that_commits_no_files_has_no_commit_entry(): void
    {
        $recording = new RecordingHeartbeat();
        $written = [];

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(to_array($written))
            ->run();

        static::assertSame([[], [['id' => 1], ['id' => 2]]], [
            array_merge(...HeartbeatContext::entries($recording, StageName::commit)),
            $written,
        ]);
    }

    public function test_a_write_to_files_has_its_commit_entry_from_the_first_beat(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))
            ->write(to_floe(path('memory://out/rows.floe'), filesystem: memory_filesystem()))
            ->run();

        static::assertSame(
            [null],
            array_map(
                static fn(StageProgress $commit): ?float => $commit->startedAtSeconds,
                HeartbeatContext::entries($recording, StageName::commit)[0],
            ),
        );
    }

    public function test_a_write_to_files_in_a_transaction_has_one_commit_entry_that_counts_its_file(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))
            ->write(
                new Transactional(
                    new RecordingTransaction(),
                    to_floe(path('memory://out/rows.floe'), filesystem: memory_filesystem()),
                ),
            )
            ->run();

        $pulses = HeartbeatContext::entries($recording, StageName::commit);
        static::assertSame(
            [[1, true]],
            array_map(
                static fn(StageProgress $commit): array => [$commit->count, $commit->startedAtSeconds !== null],
                array_pop($pulses) ?? [],
            ),
        );
    }

    public function test_a_transaction_of_a_file_and_a_memory_write_has_only_the_file_commit_entry(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))
            ->write(
                new Transactional(
                    new RecordingTransaction(),
                    to_floe(path('memory://out/rows.floe'), filesystem: memory_filesystem()),
                    to_memory(new ArrayMemory()),
                ),
            )
            ->run();

        $pulses = HeartbeatContext::entries($recording, StageName::commit);
        static::assertSame(
            [['memory://out/', 1, true]],
            array_map(
                static fn(StageProgress $commit): array => [
                    $commit->subject,
                    $commit->count,
                    $commit->startedAtSeconds !== null,
                ],
                array_pop($pulses) ?? [],
            ),
        );
    }

    public function test_a_write_to_files_in_a_branch_has_one_commit_entry_that_counts_its_file(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))
            ->write(to_branch(
                ref('id')->greaterThan(lit(1)),
                to_floe(path('memory://out/rows.floe'), filesystem: memory_filesystem()),
            ))
            ->run();

        $pulses = HeartbeatContext::entries($recording, StageName::commit);
        static::assertSame(
            [[1, true]],
            array_map(
                static fn(StageProgress $commit): array => [$commit->count, $commit->startedAtSeconds !== null],
                array_pop($pulses) ?? [],
            ),
        );
    }

    public function test_a_write_that_reports_at_its_end_lists_its_commit_entry_only_then(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(new ReportingAtEndLoader())
            ->run();

        $shapes = array_map(static fn(StageProgress $commit): array => [
            $commit->count,
            $commit->startedAtSeconds !== null,
        ], array_merge(...HeartbeatContext::entries($recording, StageName::commit)));
        static::assertSame([true, []], [
            $shapes !== [],
            array_values(array_filter($shapes, static fn(array $shape): bool => $shape !== [1, true])),
        ]);
    }

    public function test_a_stage_that_never_starts_ends_with_the_run(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1]]))
            ->filter(ref('id')->greaterThan(lit(1)))
            ->sortBy(ref('id'))
            ->write(to_floe(path('memory://out/rows.floe'), filesystem: memory_filesystem()))
            ->run();

        $sort = array_slice(HeartbeatContext::entries($recording, StageName::sort), -1)[0][0];
        static::assertSame([null, true], [$sort->startedAtSeconds, $sort->endedAtSeconds !== null]);
    }

    public function test_join_each_reports_a_join_stage_that_ends_with_its_input(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->joinEach(new StaticExtractorFactory(from_array([['id' => 1]])), join_on(['id' => 'id']))
            ->sortBy(ref('id'))
            ->run();

        $ended = array_values(array_filter(
            array_slice($recording->pulses(), -1)[0]->stages,
            static fn(StageProgress $stage): bool => (
                $stage->name === StageName::join
                || $stage->name === StageName::sort
            ),
        ));
        usort(
            $ended,
            static fn(StageProgress $left, StageProgress $right): int => (
                (float) $left->endedAtSeconds <=> (float) $right->endedAtSeconds
            ),
        );

        static::assertSame(
            ['join', 'sort'],
            array_map(static fn(StageProgress $stage): string => $stage->name->value, $ended),
        );
    }

    public function test_a_sort_counts_every_batch_it_takes(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array(RowsMother::sequentialIds(30)->toArray())->withBatchSize(10))
            ->sortBy(ref('id'))
            ->fetch();

        static::assertContains(
            [10],
            array_map(
                static fn(array $sorts): array => array_map(
                    static fn(StageProgress $sort): int => $sort->count,
                    $sorts,
                ),
                HeartbeatContext::entries($recording, StageName::sort),
            ),
        );
    }

    public function test_a_branch_write_does_not_count_its_rows_as_read_again(): void
    {
        $recording = new RecordingHeartbeat();
        $first = [];
        $second = [];

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(from_array(RowsMother::sequentialIds(30)->toArray())->withBatchSize(10))
            ->sortBy(ref('id'))
            ->write(to_branch(ref('id')->greaterThanEqual(lit(1)), to_array($first)))
            ->write(to_branch(ref('id')->greaterThanEqual(lit(1)), to_array($second)))
            ->run();

        static::assertCount(30, $second);
        static::assertSame([30, [30]], [
            array_slice($recording->pulses(), -1)[0]->rowsRead,
            array_map(
                static fn(StageProgress $read): int => $read->count,
                array_slice(HeartbeatContext::entries($recording, StageName::read), -1)[0],
            ),
        ]);
    }

    public function test_a_launched_read_is_counted_where_it_runs(): void
    {
        $recording = new RecordingHeartbeat();

        $rows = df(
            config_builder()
                ->clock(new SteppingClock())
                ->launcher(new StubLauncher())
                ->heartbeat($recording, everySeconds: 1),
        )
            ->read(new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(4)]))
            ->fetch();

        static::assertSame(4, $rows->count());
        static::assertNotSame([], $recording->pulses());
        static::assertSame(
            [],
            array_filter($recording->pulses(), static fn(Pulse $pulse): bool => $pulse->rowsRead > 0),
        );
    }

    /**
     * @return Generator<string, array{ErrorHandler}>
     */
    public static function error_handlers(): Generator
    {
        yield 'throw' => [new ThrowError()];
        yield 'ignore' => [new IgnoreError()];
        yield 'skip rows' => [new SkipRows()];
    }

    #[DataProvider('error_handlers')]
    public function test_a_heartbeat_failure_stops_the_run_whatever_the_error_handler(ErrorHandler $handler): void
    {
        $telemetry = new MemoryTelemetryContext();
        $failure = new RuntimeException('lease lost');
        $written = [];

        try {
            df(
                config_builder()
                    ->clock(new SteppingClock())
                    ->withTelemetry($telemetry->telemetry)
                    ->heartbeat(
                        new RecordingHeartbeat(throws: $failure),
                        everySeconds: 1,
                        onFailure: HeartbeatFailure::stop,
                    ),
            )
                ->read(from_array([['id' => 1], ['id' => 2]])->withBatchSize(1))
                ->onError($handler)
                ->write(to_array($written))
                ->run();
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        $telemetry->telemetry->flush();
        static::assertSame([], $telemetry->logs->entriesContaining('Error during extraction.'));
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
        $sources = new RunCounts();
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
        $sources = new RunCounts();
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

    public function test_a_failed_run_ends_its_source_while_the_failure_is_still_held(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $step = new TracedFailureTransformer();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(0, new Read($extractor), new PipelineSteps([$step])),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(),
            ));

            static::fail('Expected the step failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertSame([], $extractor->signals);
            static::assertSame(1, $extractor->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_failed_run_discards_its_loaders_and_never_closes_them(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $loader = new RecordingLoader();
        $step = new TracedFailureTransformer();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(0, new Read($extractor), new PipelineSteps([$loader, $step])),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(),
            ));

            static::fail('Expected the step failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertSame(['load#1(1)', 'discard'], $loader->log);
            static::assertSame(1, $extractor->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_failed_run_discards_the_loaders_of_a_run_before_a_processor(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $loader = new RecordingLoader();
        $step = new TracedFailureTransformer();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(
                        0,
                        new Read($extractor),
                        new PipelineSteps([$loader, new BatchingProcessor(new BatchSize(1)), $step]),
                    ),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(),
            ));

            static::fail('Expected the failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertSame(['load#1(1)', 'discard'], $loader->log);
            static::assertSame(1, $extractor->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_failed_run_discards_the_loaders_of_runs_two_processors_before_it(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $loader = new RecordingLoader();
        $step = new TracedFailureTransformer();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(
                        0,
                        new Read($extractor),
                        new PipelineSteps([
                            $loader,
                            new BatchingProcessor(new BatchSize(1)),
                            new BatchingProcessor(new BatchSize(1)),
                            $step,
                        ]),
                    ),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(),
            ));

            static::fail('Expected the failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertSame(['load#1(1)', 'discard'], $loader->log);
            static::assertSame(1, $extractor->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_failed_run_discards_the_loaders_of_a_run_before_a_delegating_processor(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $loader = new RecordingLoader();
        $step = new TracedFailureTransformer();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(
                        0,
                        new Read($extractor),
                        new PipelineSteps([$loader, new PassThroughProcessor(), $step]),
                    ),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(),
            ));

            static::fail('Expected the failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertSame(['load#1(1)', 'discard'], $loader->log);
            static::assertSame(1, $extractor->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_processor_that_fails_discards_the_loaders_of_the_run_before_it(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $loader = new RecordingLoader();
        $processor = new TracedFailureProcessor();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(0, new Read($extractor), new PipelineSteps([$loader, $processor])),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(),
            ));

            static::fail('Expected the failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($processor->thrown, $e);
            static::assertSame(['load#1(1)', 'discard'], $loader->log);
            static::assertSame(1, $extractor->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_failed_launched_run_ends_the_launch_while_the_failure_is_still_held(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $step = new TracedFailureTransformer();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(0, new Read($extractor), new PipelineSteps()),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(
                    config_builder()->launcher(new StubLauncher(rest: new PipelineSteps([$step])))->build(),
                ),
            ));

            static::fail('Expected the step failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertSame([], $extractor->signals);
            static::assertSame(1, $extractor->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_failed_launched_run_discards_the_launchs_loaders_and_never_closes_them(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $loader = new RecordingLoader();
        $step = new TracedFailureTransformer();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(0, new Read($extractor), new PipelineSteps()),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(
                    config_builder()
                        ->launcher(new StubLauncher(new PipelineSteps([$loader]), new PipelineSteps([$step])))
                        ->build(),
                ),
            ));

            static::fail('Expected the step failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertSame(['load#1(1)', 'discard'], $loader->log);
            static::assertSame(1, $extractor->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_failed_launched_run_discards_its_parent_side_loaders_and_never_closes_them(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $loader = new RecordingLoader();
        $step = new TracedFailureTransformer();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(0, new Read($extractor), new PipelineSteps()),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(
                    config_builder()->launcher(new StubLauncher(rest: new PipelineSteps([$loader, $step])))->build(),
                ),
            ));

            static::fail('Expected the step failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertSame(['load#1(1)', 'discard'], $loader->log);
            static::assertSame(1, $extractor->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_failed_run_ends_a_launch_that_delegates_its_batches(): void
    {
        $step = new TracedFailureTransformer();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(0, new Read($extractor), new PipelineSteps()),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(
                    config_builder()->launcher(new DelegatingLauncher(new PipelineSteps([$step])))->build(),
                ),
            ));

            static::fail('Expected the step failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertSame(1, $extractor->cleanups);
        }
    }

    public function test_a_join_whose_right_side_fails_ends_the_right_source_and_never_starts_the_left(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $step = new TracedFailureTransformer();
        $left = new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(1)]);
        $right = new RecordingSplittableExtractor(schema(int_schema('id')), [
            RowsMother::sequentialIds(1),
            RowsMother::sequentialIds(1),
        ]);

        try {
            (new Executor())->fetch(
                NodeMother::plan(NodeMother::join(
                    NodeMother::read($left),
                    NodeMother::joinRight(NodeMother::plan(new Transform(NodeMother::read($right), $step))),
                )),
                NodeMother::context(),
            );

            static::fail('Expected the right side failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertSame([], $left->limits);
            static::assertSame(1, $right->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_join_that_fails_while_it_builds_ends_the_right_source(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $left = new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(1)]);
        $right = new RecordingSplittableExtractor(schema(str_schema('id')), [
            array_to_rows([['id' => 'not a number']], schema(str_schema('id'))),
            array_to_rows([['id' => '2']], schema(str_schema('id'))),
        ]);

        try {
            (new Executor())->fetch(
                NodeMother::plan(NodeMother::join(
                    NodeMother::read($left),
                    NodeMother::joinRight(NodeMother::plan(NodeMother::read($right))),
                )),
                NodeMother::context(config_builder()->join(hash_join()->memoryLimit(Unit::fromBytes(1)))->build()),
            );

            static::fail('Expected the join build failure to surface.');
        } catch (EvaluationException $e) {
            static::assertStringStartsWith('Column "id" value \'not a number\' cannot be cast', $e->getMessage());
            static::assertSame([null], $right->signals);
            static::assertSame(1, $right->cleanups);
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_a_processor_that_fails_before_reading_under_another_never_starts_the_source(): void
    {
        $failure = new RuntimeException('processor exploded');
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(1)]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(
                        0,
                        new Read($extractor),
                        new PipelineSteps([
                            new EagerFailureProcessor($failure),
                            new BatchingProcessor(new BatchSize(1)),
                        ]),
                    ),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(),
            ));

            static::fail('Expected the processor failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
            static::assertSame([], $extractor->limits);
        }
    }

    public function test_a_launch_that_fails_to_end_leaves_the_run_failure_to_surface(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $step = new TracedFailureTransformer();
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(1)]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(0, new Read($extractor), new PipelineSteps()),
                    schema(int_schema('id')),
                    null,
                ),
                NodeMother::context(
                    config_builder()
                        ->withTelemetry($telemetry->telemetry)
                        ->launcher(
                            new DelegatingLauncher(
                                new PipelineSteps([$step]),
                                new RuntimeException('teardown exploded'),
                            ),
                        )
                        ->build(),
                ),
            ));

            static::fail('Expected the step failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($step->thrown, $e);
            static::assertNull($e->getPrevious());
            static::assertSame(1, $extractor->cleanups);
            static::assertCount(1, $telemetry->logs->entriesWithSeverity(Severity::WARN));
            static::assertCount(1, $telemetry->logs->entriesContaining('The input of a failed run failed to end'));
        }
    }

    public function test_a_run_that_fails_after_its_source_ended_leaves_it_ended(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $failure = new RuntimeException('closure exploded');
        $extractor = new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(1)]);

        try {
            iterator_to_array((new Executor())->executePlan(
                new PhysicalPlan(
                    new Pipeline(0, new Read($extractor), new PipelineSteps([new ClosureThrowingLoader($failure)])),
                    schema(int_schema('id')),
                    null,
                ),
                $telemetry->flowContext,
            ));

            static::fail('Expected the loader failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
            static::assertSame([null], $extractor->signals);
            static::assertSame(1, $extractor->cleanups);
            static::assertSame([], $telemetry->logs->entriesWithSeverity(Severity::WARN));
        }
    }

    public function test_a_heartbeat_stop_while_a_write_ends_keeps_the_moved_and_the_replaced_files(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $heartbeat = new RecordingHeartbeat(throws: $failure);
        $memory = memory_filesystem();
        $memory->writeTo(path('memory://out/part-00007.floe'))->append('old')->close();
        $filesystem = $context->slowMoves($memory);

        try {
            df(
                config_builder()
                    ->clock($context->clock)
                    ->heartbeat($heartbeat, everySeconds: 10, onFailure: HeartbeatFailure::stop),
            )
                ->read(from_array([['id' => 1]]))
                ->write(to_floe(path('memory://out/part-{part}.floe'), filesystem: $filesystem)->saveMode(overwrite()))
                ->run();
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame(
            ['commit'],
            array_map(
                static fn(Pulse $pulse): string => array_slice($pulse->stages, -1)[0]->name->value,
                $heartbeat->pulses(),
            ),
        );
        static::assertSame(
            ['memory://out/part-00000.floe', 'memory://out/part-00007.floe'],
            array_map(
                static fn(FileStatus $status): string => $status->path->uri(),
                iterator_to_array($memory->list(path('memory://out/**/*')), false),
            ),
        );
    }

    public function test_a_heartbeat_stop_while_writes_end_discards_the_writes_not_ended_yet(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $heartbeat = new RecordingHeartbeat(throws: $failure);
        $memory = memory_filesystem();
        $memory->writeTo(path('memory://b/part-00007.floe'))->append('old')->close();
        $filesystem = $context->slowMoves($memory);

        try {
            df(
                config_builder()
                    ->clock($context->clock)
                    ->heartbeat($heartbeat, everySeconds: 10, onFailure: HeartbeatFailure::stop),
            )
                ->read(from_array([['id' => 1]]))
                ->write(to_floe(path('memory://a/part-{part}.floe'), filesystem: $filesystem)->saveMode(overwrite()))
                ->write(to_floe(path('memory://b/part-{part}.floe'), filesystem: $filesystem)->saveMode(overwrite()))
                ->run();
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame(
            ['commit'],
            array_map(
                static fn(Pulse $pulse): string => array_slice($pulse->stages, -1)[0]->name->value,
                $heartbeat->pulses(),
            ),
        );
        static::assertSame(
            ['memory://a/part-00000.floe', 'memory://b/part-00007.floe'],
            array_map(
                static fn(FileStatus $status): string => $status->path->uri(),
                iterator_to_array($memory->list(path('memory://*/**/*')), false),
            ),
        );
    }

    public function test_a_heartbeat_stop_inside_a_branch_discards_the_writes_after_it(): void
    {
        $context = new HeartbeatContext();
        $failure = new RuntimeException('lease lost');
        $heartbeat = new RecordingHeartbeat(throws: $failure);
        $memory = memory_filesystem();
        $memory->writeTo(path('memory://b/part-00007.floe'))->append('old')->close();
        $filesystem = $context->slowMoves($memory);

        try {
            df(
                config_builder()
                    ->clock($context->clock)
                    ->heartbeat($heartbeat, everySeconds: 10, onFailure: HeartbeatFailure::stop),
            )
                ->read(from_array([['id' => 1]]))
                ->write(to_branch(
                    ref('id')->isNotNull(),
                    to_floe(path('memory://a/part-{part}.floe'), filesystem: $filesystem)->saveMode(overwrite()),
                ))
                ->write(to_floe(path('memory://b/part-{part}.floe'), filesystem: $filesystem)->saveMode(overwrite()))
                ->run();
            static::fail('The heartbeat failure did not stop the run');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame(
            ['commit'],
            array_map(
                static fn(Pulse $pulse): string => array_slice($pulse->stages, -1)[0]->name->value,
                $heartbeat->pulses(),
            ),
        );
        static::assertSame(
            ['memory://a/part-00000.floe', 'memory://b/part-00007.floe'],
            array_map(
                static fn(FileStatus $status): string => $status->path->uri(),
                iterator_to_array($memory->list(path('memory://*/**/*')), false),
            ),
        );
    }

    public function test_an_overwrite_beats_while_its_files_move_into_place(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock($context->clock)->heartbeat($recording, everySeconds: 10))
            ->read(from_array([['id' => 1, 'dept' => 'a'], ['id' => 2, 'dept' => 'b'], ['id' => 3, 'dept' => 'c']]))
            ->write(
                to_floe(path('memory://out/rows.floe'), filesystem: $context->slowMoves(memory_filesystem()))
                    ->partitionBy(partition_by('dept'))
                    ->saveMode(overwrite()),
            )
            ->run();

        static::assertSame(
            [['commit', 10.0], ['commit', 20.0], ['commit', 30.0], ['commit', 30.0]],
            array_map(static fn(Pulse $pulse): array => [
                array_slice($pulse->stages, -1)[0]->name->value,
                $pulse->elapsedSeconds,
            ], $recording->pulses()),
        );
    }

    public function test_two_sorts_are_two_sort_entries(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(
                from_array(array_map(static fn(int $id): array => ['id' => $id, 'g' => $id % 3], range(1, 12)))
                    ->withBatchSize(2),
            )
            ->sortBy(ref('id')->desc())
            ->sortBy(ref('g'))
            ->run();

        $sorts = HeartbeatContext::entries($recording, StageName::sort);
        $last = array_slice($sorts, -1)[0];
        static::assertCount(2, $last);
        static::assertNotSame($last[0]->id, $last[1]->id);
        static::assertSame([12, 12], [$last[0]->count, $last[1]->count]);
        static::assertNotNull($last[0]->endedAtSeconds);
        static::assertNotNull($last[1]->endedAtSeconds);

        $counts = [];

        foreach ($sorts as $entries) {
            foreach ($entries as $entry) {
                $counts[(int) $entry->id][] = $entry->count;
            }
        }

        foreach ($counts as $byId) {
            $grown = $byId;
            sort($grown);
            static::assertSame($grown, $byId);
        }
    }

    public function test_each_write_is_its_own_commit_entry(): void
    {
        $context = new HeartbeatContext();
        $recording = new RecordingHeartbeat();
        $memory = memory_filesystem();
        $filesystem = $context->slowMoves($memory);

        df(config_builder()->clock($context->clock)->heartbeat($recording, everySeconds: 10))
            ->read(from_array([['id' => 1, 'dept' => 'x'], ['id' => 2, 'dept' => 'y']]))
            ->write(to_floe(path('memory://a/part-{part}.floe'), filesystem: $filesystem)->saveMode(overwrite()))
            ->write(to_branch(
                ref('id')->greaterThanEqual(lit(1)),
                to_floe(path('memory://b/part-{part}.floe'), filesystem: $filesystem)->saveMode(overwrite()),
            ))
            ->write(to_floe(path('memory://c/part-{part}.floe'), filesystem: $filesystem)->saveMode(overwrite()))
            ->run();

        $commits = HeartbeatContext::entries($recording, StageName::commit);
        $last = array_slice($commits, -1)[0];
        static::assertSame(
            ['memory://a/', 'memory://b/', 'memory://c/'],
            array_map(static fn(StageProgress $commit): ?string => $commit->subject, $last),
        );

        foreach ($last as $commit) {
            static::assertCount(
                $commit->count,
                array_filter(
                    iterator_to_array($memory->list(path((string) $commit->subject . '**/*')), false),
                    static fn(FileStatus $status): bool => $status->isFile(),
                ),
            );
        }

        $counts = [];

        foreach ($commits as $entries) {
            foreach ($entries as $entry) {
                $counts[(int) $entry->id][] = $entry->count;
            }
        }

        foreach ($counts as $byId) {
            $grown = $byId;
            sort($grown);
            static::assertSame($grown, $byId);
        }
    }

    public function test_two_reads_are_two_read_entries(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60))
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->join(df()->read(from_array([['id' => 1], ['id' => 2]])), join_on(['id' => 'id'], 'r_'))
            ->run();

        $last = array_slice(HeartbeatContext::entries($recording, StageName::read), -1)[0];
        static::assertEqualsCanonicalizing(
            [3, 2],
            array_map(static fn(StageProgress $read): ?int => $read->total, $last),
        );
        static::assertSame([null, null], array_map(static fn(StageProgress $read): ?string => $read->subject, $last));
    }

    public function test_a_file_read_has_its_location(): void
    {
        $recording = new RecordingHeartbeat();
        $memory = memory_filesystem();
        df()
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->write(to_floe(path('memory://in/orders.floe'), filesystem: $memory))
            ->run();

        df(config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60))
            ->read(from_floe(path('memory://in/orders.floe'), filesystem: $memory))->run();

        static::assertSame(
            ['memory://in/orders.floe'],
            array_map(
                static fn(StageProgress $read): ?string => $read->subject,
                HeartbeatContext::entries($recording, StageName::read)[0],
            ),
        );
    }

    public function test_a_read_location_keeps_no_user_password_query_or_fragment(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60))
            ->read((new RecordingFileExtractor(
                schema(int_schema('id')),
                RowsMother::sequentialIds(2),
            ))->withSource(path('sftp://user:secret@host:22/in/orders.csv?X-Amz-Signature=abc&token=t#frag')))->run();

        static::assertSame(
            ['sftp://host/in/orders.csv'],
            array_map(
                static fn(StageProgress $read): ?string => $read->subject,
                HeartbeatContext::entries($recording, StageName::read)[0],
            ),
        );
    }

    public function test_a_completed_run_beats_once_more_with_every_stage_ended(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60))
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 2]]))
            ->sortBy(ref('id'))
            ->run();

        static::assertCount(1, $recording->pulses());
        static::assertNotSame([], $recording->pulses()[0]->stages);

        foreach ($recording->pulses()[0]->stages as $stage) {
            static::assertNotNull($stage->endedAtSeconds);
        }
    }

    public function test_a_failed_run_does_not_beat_at_its_end(): void
    {
        $recording = new RecordingHeartbeat();
        $failure = new RuntimeException('transformation failed');

        try {
            df(config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60))
                ->read(from_array([['id' => 3], ['id' => 1], ['id' => 2]]))
                ->sortBy(ref('id'))
                ->with(new ThrowingTransformer($failure))
                ->run();
            static::fail('The transformer failure did not reach the caller');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertSame([], $recording->pulses());
    }

    public function test_the_read_of_an_exact_source_has_its_total(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60))
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))->run();

        static::assertSame(
            [3],
            array_map(
                static fn(StageProgress $read): ?int => $read->total,
                HeartbeatContext::entries($recording, StageName::read)[0],
            ),
        );
    }

    public function test_a_limited_read_has_no_total(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60))
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->limit(1)
            ->run();

        static::assertSame(
            [null],
            array_map(
                static fn(StageProgress $read): ?int => $read->total,
                HeartbeatContext::entries($recording, StageName::read)[0],
            ),
        );
    }

    public function test_a_group_by_counts_the_rows_it_takes_then_the_groups_it_emits(): void
    {
        $recording = new RecordingHeartbeat();

        df(config_builder()->clock(new SteppingClock())->heartbeat($recording, everySeconds: 1))
            ->read(
                from_array(array_map(static fn(int $id): array => ['id' => $id, 'g' => $id % 3], range(1, 12)))
                    ->withBatchSize(2),
            )
            ->groupBy(ref('g'))
            ->aggregate(count(ref('id')))
            ->run();

        $shown = array_map(static fn(array $entries): array => [
            $entries[0]->unit,
            $entries[0]->count,
        ], array_values(array_filter(HeartbeatContext::entries($recording, StageName::group_by), static fn(array $entries): bool => $entries !== [])));
        static::assertContains([ProgressUnit::rows, 6], $shown);
        static::assertSame([ProgressUnit::groups, 3], array_slice($shown, -1)[0]);
    }

    public function test_entries_are_in_the_order_of_the_pipeline_as_written(): void
    {
        $recording = new RecordingHeartbeat();
        $memory = memory_filesystem();

        df(config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60))
            ->read(from_array([['id' => 1, 'g' => 1], ['id' => 2, 'g' => 2], ['id' => 3, 'g' => 1]]))
            ->join(df()->read(from_array([['id' => 1], ['id' => 2]])), join_on(['id' => 'id'], 'r_'))
            ->write(to_floe(path('memory://joined/part-{part}.floe'), filesystem: $memory))
            ->sortBy(ref('id'))
            ->groupBy(ref('g'))
            ->aggregate(count(ref('id')))
            ->write(to_floe(path('memory://grouped/part-{part}.floe'), filesystem: $memory))
            ->run();

        static::assertSame(
            [
                ['read',     3,    null],
                ['read',     2,    null],
                ['join',     null, 'left on id'],
                ['write',    null, 'memory://joined/'],
                ['commit',   null, 'memory://joined/'],
                ['sort',     null, null],
                ['group_by', null, null],
                ['write',    null, 'memory://grouped/'],
                ['commit',   null, 'memory://grouped/'],
            ],
            array_map(static fn(StageProgress $stage): array => [
                $stage->name->value,
                $stage->total,
                $stage->subject,
            ], array_values(array_filter(array_slice($recording->pulses(), -1)[0]->stages, static fn(StageProgress $stage): bool => $stage->name !== StageName::infer))),
        );
    }

    public function test_a_second_run_counts_in_stages_of_its_own(): void
    {
        $recording = new RecordingHeartbeat();
        $frame = df(config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60))
            ->read(from_array([['id' => 3], ['id' => 1], ['id' => 2]]))
            ->sortBy(ref('id'));

        $frame->run();
        $frame->run();

        static::assertSame(
            [[['infer', 3], ['read', 3], ['sort', 3]], [['read', 3], ['sort', 3]]],
            array_map(static fn(Pulse $pulse): array => array_map(static fn(StageProgress $stage): array => [
                $stage->name->value,
                $stage->count,
            ], $pulse->stages), $recording->pulses()),
        );
    }

    public function test_without_a_heartbeat_no_write_location_is_built(): void
    {
        $plain = new CountingFileLoader(to_floe(
            path('memory://plain/part-{part}.floe'),
            filesystem: memory_filesystem(),
        ));
        $watched = new CountingFileLoader(to_floe(
            path('memory://watched/part-{part}.floe'),
            filesystem: memory_filesystem(),
        ));

        df()
            ->read(from_array([['id' => 1]]))
            ->write($plain)
            ->run();
        df(config_builder()->clock(new FakeClock())->heartbeat(new RecordingHeartbeat(), everySeconds: 60))
            ->read(from_array([['id' => 1]]))
            ->write($watched)
            ->run();

        static::assertSame(
            [1, 2],
            [$plain->destinationCalls->count(), $watched->destinationCalls->count()],
            'the planner asks a write for its destination once; a heartbeat adds the location of its commit',
        );
    }
}
