<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\Operators\Join\CrossJoin;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Operators\Processor\OffsetProcessor;
use Flow\ETL\Operators\Processor\VoidProcessor;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Operators\Transformer\UntilTransformer;
use Flow\ETL\Planning\Physical\JoinStep;
use Flow\ETL\Planning\Physical\PipelineSteps;
use Flow\ETL\Runtime\ErrorHandler;
use Flow\ETL\Runtime\ErrorHandler\ExtractionError;
use Flow\ETL\Runtime\ErrorHandler\IgnoreError;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\SkipRows;
use Flow\ETL\Runtime\ErrorHandler\ThrowError;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;
use Flow\ETL\Runtime\Executor\StepRunner;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\RecordingErrorHandler;
use Flow\ETL\Tests\Double\RecordingLoader;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Rows;
use Flow\Telemetry\Logger\LogEntry;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;

use function array_map;
use function array_merge;
use function array_slice;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class StepRunnerTest extends FlowTestCase
{
    public function test_a_run_drops_empty_batches_and_closes_its_loaders_when_it_completes(): void
    {
        $loader = new RecordingLoader();

        static::assertSame(
            [[['id' => 1], ['id' => 2]]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    (new StepRunner())->run(
                        [$loader],
                        (static function (): Generator {
                            yield rows(schema(int_schema('id')));
                            yield RowsMother::sequentialIds(2);
                        })(),
                        flow_context(config()),
                    ),
                    false,
                ),
            ),
        );
        static::assertSame(['closure'], array_slice($loader->log, -1));
    }

    public function test_ending_an_incomplete_run_discards_its_loaders(): void
    {
        $loader = new RecordingLoader();

        static::assertSame([], (new StepRunner())->endLoaders([$loader], flow_context(config()), false));
        static::assertSame(['discard'], $loader->log);
    }

    public function test_a_failing_closure_is_returned_and_its_loader_discarded(): void
    {
        $failure = new RuntimeException('closure failed');
        $loader = new RecordingLoader(closureFailure: $failure);

        static::assertSame([$failure], (new StepRunner())->endLoaders([$loader], flow_context(config()), true));
        static::assertSame(['closure THROW', 'discard'], $loader->log);
    }

    public function test_steps_without_a_source_rethrow_their_input_error_untouched(): void
    {
        $handler = new RecordingErrorHandler();
        $failure = new RuntimeException('upstream failed');

        $thrown = null;

        try {
            iterator_to_array((new StepRunner())->steps(
                new PipelineSteps(),
                (static function () use ($failure): Generator {
                    yield RowsMother::sequentialIds(1);

                    throw $failure;
                })(),
                flow_context(config())->withErrorHandler($handler),
            ));
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertSame($failure, $thrown);
        static::assertSame([], $handler->errors);
    }

    public function test_extraction_error_after_the_first_batch_reaches_the_handler(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('id')));
        $handler = new RecordingErrorHandler();
        $loader = new SpyLoader();

        static::assertSame(
            [[['id' => 1], ['id' => 2]]],
            array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    (new StepRunner())->steps(
                        new PipelineSteps([$loader]),
                        (static function (): Generator {
                            yield RowsMother::sequentialIds(2);

                            throw new RuntimeException('second batch failed');
                        })(),
                        flow_context(config())->withErrorHandler($handler),
                        $extractor,
                    ),
                    false,
                ),
            ),
        );
        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(ExtractionError::class, $handler->errors[0]);
        static::assertSame($extractor, $handler->errors[0]->extractor);
        static::assertSame('second batch failed', $handler->errors[0]->cause->getMessage());
        static::assertSame(1, $loader->closureCount);
    }

    public function test_extraction_error_propagates_under_throw_error(): void
    {
        $steps = new PipelineSteps();
        $extractor = new CountingExtractor(schema(int_schema('id')));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('source failed');

        iterator_to_array((new StepRunner())->steps(
            $steps,
            (static function (): Generator {
                yield from [];

                throw new RuntimeException('source failed');
            })(),
            flow_context(config())->withErrorHandler(new ThrowError()),
            $extractor,
        ));
    }

    public function test_extraction_error_reaches_the_handler(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('id')));
        $handler = new RecordingErrorHandler();
        $loader = new SpyLoader();

        static::assertSame(
            [],
            iterator_to_array((new StepRunner())->steps(
                new PipelineSteps([$loader]),
                (static function (): Generator {
                    yield from [];

                    throw new RuntimeException('source failed');
                })(),
                flow_context(config())->withErrorHandler($handler),
                $extractor,
            )),
        );
        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(ExtractionError::class, $handler->errors[0]);
        static::assertSame($extractor, $handler->errors[0]->extractor);
        static::assertSame(1, $loader->closureCount);
    }

    public function test_ignore_error_drops_the_half_transformed_batch(): void
    {
        $handler = new RecordingErrorHandler();

        static::assertSame(
            [],
            iterator_to_array((new StepRunner())->steps(
                new PipelineSteps([
                    new ThrowingTransformer(new RuntimeException('first')),
                    new ThrowingTransformer(new RuntimeException('second')),
                ]),
                (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))->extract(
                    flow_context()->backend(),
                    new Criteria(),
                ),
                flow_context(config())->withErrorHandler($handler),
            )),
        );
        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(TransformationError::class, $handler->errors[0]);
        static::assertSame('first', $handler->errors[0]->cause->getMessage());
    }

    public function test_loading_error_skips_only_the_failing_loader(): void
    {
        $handler = new RecordingErrorHandler();
        $throwing = new ThrowingLoader(new RuntimeException('write failed'));
        $spy = new SpyLoader();

        iterator_to_array((new StepRunner())->steps(
            new PipelineSteps([$throwing, $spy]),
            (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))->extract(
                flow_context()->backend(),
                new Criteria(),
            ),
            flow_context(config())->withErrorHandler($handler),
        ));

        static::assertSame(1, $throwing->loadsCount);
        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], $spy->loadedRowsToArray());
        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(LoadingError::class, $handler->errors[0]);
        static::assertSame($throwing, $handler->errors[0]->loader);
    }

    public function test_skip_rows_drops_the_batch_and_continues_with_the_next(): void
    {
        static::assertSame(
            [['id' => 1], ['id' => 3]],
            array_merge(...array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    (new StepRunner())->steps(
                        new PipelineSteps([new ThrowWhenRowMatches('id', 2, new RuntimeException('boom'))]),
                        (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))
                            ->withBatchSize(1)
                            ->extract(flow_context()->backend(), new Criteria()),
                        flow_context(config())->withErrorHandler(new SkipRows()),
                    ),
                    false,
                ),
            )),
        );
    }

    public function test_stop_is_forwarded_upstream_without_pulling_another_batch(): void
    {
        $extractor = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))->withBatchSize(1);

        static::assertCount(1, iterator_to_array(
            (new StepRunner())->steps(
                new PipelineSteps([new LimitTransformer(1)]),
                $extractor->extract(flow_context()->backend(), new Criteria()),
                flow_context(config()),
            ),
            false,
        ));
        static::assertSame(1, $extractor->batchesYielded);
    }

    public function test_a_limit_reached_at_a_batch_boundary_stops_the_source_before_the_next_batch(): void
    {
        $extractor = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(6)))->withBatchSize(2);

        static::assertSame(
            [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4]],
            array_merge(...array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    (new StepRunner())->steps(
                        new PipelineSteps([new LimitTransformer(4)]),
                        $extractor->extract(flow_context()->backend(), new Criteria()),
                        flow_context(config()),
                    ),
                    false,
                ),
            )),
        );
        static::assertSame(2, $extractor->batchesYielded);
    }

    public function test_until_failing_on_the_first_row_yields_nothing_and_stops_the_source(): void
    {
        $extractor = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))->withBatchSize(1);
        $loader = new SpyLoader();

        static::assertSame(
            [],
            iterator_to_array((new StepRunner())->steps(
                new PipelineSteps([new UntilTransformer(ref('id')->lessThan(lit(1))), $loader]),
                $extractor->extract(flow_context()->backend(), new Criteria()),
                flow_context(config()),
            )),
        );
        static::assertSame(1, $extractor->batchesYielded);
        static::assertSame([], $loader->loadedRowsToArray());
    }

    public function test_a_batch_skipped_after_a_reached_limit_still_stops_the_source(): void
    {
        $extractor = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(6)))->withBatchSize(2);

        static::assertSame(
            [],
            iterator_to_array((new StepRunner())->steps(
                new PipelineSteps([
                    new LimitTransformer(2),
                    new ThrowingTransformer(new RuntimeException('after the limit')),
                ]),
                $extractor->extract(flow_context()->backend(), new Criteria()),
                flow_context(config())->withErrorHandler(new SkipRows()),
            )),
        );
        static::assertSame(1, $extractor->batchesYielded);
    }

    public function test_every_limit_reached_in_one_batch_is_logged_with_its_step(): void
    {
        $telemetry = new MemoryTelemetryContext();

        static::assertCount(
            3,
            array_merge(...array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    (new StepRunner())->steps(
                        new PipelineSteps([new LimitTransformer(5), new LimitTransformer(3)]),
                        (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(10)))->extract(
                            flow_context()->backend(),
                            new Criteria(),
                        ),
                        $telemetry->flowContext,
                    ),
                    false,
                ),
            )),
        );
        static::assertSame(
            [LimitTransformer::class, LimitTransformer::class],
            array_map(static fn(LogEntry $entry): mixed => $entry->record->attributes->get(
                'step',
            ), $telemetry->logs->entriesContaining('Limit reached')),
        );
    }

    public function test_a_propagated_extraction_error_is_logged(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $steps = new PipelineSteps();
        $extractor = new CountingExtractor(schema(int_schema('id')));

        $thrown = null;

        try {
            iterator_to_array((new StepRunner())->steps(
                $steps,
                (static function (): Generator {
                    yield from [];

                    throw new RuntimeException('source failed');
                })(),
                $telemetry->flowContext,
                $extractor,
            ));
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(RuntimeException::class, $thrown);
        static::assertCount(1, $telemetry->logs->entriesContaining('Error during extraction.'));
    }

    public function test_steps_hand_the_rows_of_a_joins_right_plan_to_the_join(): void
    {
        static::assertSame(
            [['id' => 1, 'r_name' => 'Alice'], ['id' => 2, 'r_name' => 'Alice']],
            iterator_to_array(
                (new StepRunner())->steps(
                    new PipelineSteps([new JoinStep(
                        new CrossJoin('r_'),
                        PhysicalPlanMother::reading(from_rows(array_to_rows([[
                            'name' => 'Alice',
                        ]], schema(str_schema('name'))))),
                    )]),
                    (static function () {
                        yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
                    })(),
                    flow_context(config()),
                ),
                false,
            )[0]->toArray(),
        );
    }

    public function test_steps_run_streaming_processors_and_the_breaker_in_order(): void
    {
        $spy = new SpyLoader();

        static::assertSame(
            [[['id' => 3], ['id' => 4], ['id' => 5]]],
            array_map(
                static fn(Rows $batch): array => $batch->toArray(),
                iterator_to_array(
                    (new StepRunner())->steps(
                        new PipelineSteps([new OffsetProcessor(2), $spy], new CollectingProcessor()),
                        (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(5)))
                            ->withBatchSize(2)
                            ->extract(flow_context()->backend(), new Criteria()),
                        flow_context(config()),
                    ),
                    false,
                ),
            ),
        );
        static::assertSame([['id' => 3], ['id' => 4], ['id' => 5]], $spy->loadedRowsToArray());
    }

    public function test_an_empty_batch_the_breaker_emits_is_dropped(): void
    {
        static::assertSame(
            [],
            iterator_to_array(
                (new StepRunner())->steps(
                    new PipelineSteps([], new VoidProcessor()),
                    (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(2)))->extract(
                        flow_context()->backend(),
                        new Criteria(),
                    ),
                    flow_context(config()),
                ),
                false,
            ),
        );
    }

    public function test_a_loader_before_a_processor_is_closed_before_the_processor_emits(): void
    {
        $spy = new SpyLoader();
        $steps = (new StepRunner())->steps(
            new PipelineSteps([$spy, new BatchingProcessor(new BatchSize(10))]),
            (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))
                ->withBatchSize(1)
                ->extract(flow_context()->backend(), new Criteria()),
            flow_context(config()),
        );

        $steps->current();

        static::assertSame(1, $spy->closureCount);
        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], $steps->current()->toArray());
    }

    public function test_a_corrupted_record_is_loaded_into_the_sources_reject_loader_before_its_batch_rows(): void
    {
        $rejects = new SpyLoader();
        $loader = new SpyLoader();
        $extractor = from_array([['id' => 1], ['id' => 'x'], ['id' => 3]], schema(int_schema('id')))
            ->withBatchSize(2)
            ->withCorruptedRecords($rejects);

        iterator_to_array((new StepRunner())->steps(
            new PipelineSteps([$loader]),
            $extractor->extract(flow_context()->backend(), new Criteria()),
            flow_context(config()),
            $extractor,
        ));

        static::assertSame([['id' => 1], ['id' => 3]], $loader->loadedRowsToArray());
        static::assertSame(
            [[
                'uri' => null,
                'offset' => 2,
                'line' => null,
                'reason' => 'column "id": could not convert \'x\' (string) to integer',
                'record' => '{"id":"x"}',
                'fields' => null,
            ]],
            $rejects->loadedRowsToArray(),
        );
        static::assertSame(1, $rejects->closureCount);
    }

    public function test_corrupted_records_left_after_the_last_rows_are_loaded_before_the_reject_loader_closes(): void
    {
        $rejects = new RecordingLoader();
        $extractor = from_array([['id' => 'x'], ['id' => 'y']], schema(int_schema('id')))
            ->withBatchSize(10)
            ->withCorruptedRecords($rejects);

        static::assertSame(
            [],
            iterator_to_array((new StepRunner())->steps(
                new PipelineSteps(),
                $extractor->extract(flow_context()->backend(), new Criteria()),
                flow_context(config()),
                $extractor,
            )),
        );
        static::assertSame(['load#1(2)', 'closure'], $rejects->log);
    }

    public function test_without_a_reject_loader_a_corrupted_record_is_thrown_as_an_extraction_error(): void
    {
        $extractor = from_array([['id' => 'x']], schema(int_schema('id')));

        $this->expectException(CorruptedRecordException::class);

        iterator_to_array((new StepRunner())->steps(
            new PipelineSteps(),
            $extractor->extract(flow_context()->backend(), new Criteria()),
            flow_context(config())->withErrorHandler(new ThrowError()),
            $extractor,
        ));
    }

    /**
     * @return Generator<string, array{ErrorHandler}>
     */
    public static function skipping_error_handlers(): Generator
    {
        yield 'skip rows' => [new SkipRows()];
        yield 'ignore error' => [new IgnoreError()];
    }

    #[DataProvider('skipping_error_handlers')]
    public function test_without_a_reject_loader_a_corrupted_record_fails_the_run_whatever_the_error_handler(ErrorHandler $decides): void
    {
        $handler = new RecordingErrorHandler($decides);
        $extractor = from_array([['id' => 'x'], ['id' => 2]], schema(int_schema('id')));
        $thrown = null;

        try {
            iterator_to_array((new StepRunner())->steps(
                new PipelineSteps(),
                $extractor->extract(flow_context()->backend(), new Criteria()),
                flow_context(config())->withErrorHandler($handler),
                $extractor,
            ));
        } catch (CorruptedRecordException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(CorruptedRecordException::class, $thrown);
        static::assertSame([], $handler->errors);
    }

    public function test_a_failed_run_discards_the_reject_loader(): void
    {
        $rejects = new RecordingLoader();
        $extractor = from_array([['id' => 'x'], ['id' => 2]], schema(int_schema('id')))
            ->withBatchSize(2)
            ->withCorruptedRecords($rejects);

        try {
            iterator_to_array((new StepRunner())->steps(
                new PipelineSteps([new RecordingLoader(new RuntimeException('write failed'))]),
                $extractor->extract(flow_context()->backend(), new Criteria()),
                flow_context(config())->withErrorHandler(new ThrowError()),
                $extractor,
            ));
            static::fail('Expected the write to fail the run');
        } catch (RuntimeException) {
        }

        static::assertSame(['load#1(1)', 'discard'], $rejects->log);
    }

    public function test_a_failing_reject_loader_reaches_the_loading_handler(): void
    {
        $handler = new RecordingErrorHandler();
        $rejects = new RecordingLoader(new RuntimeException('rejects failed'));
        $extractor = from_array([['id' => 'x'], ['id' => 2]], schema(int_schema('id')))
            ->withBatchSize(2)
            ->withCorruptedRecords($rejects);

        static::assertCount(
            1,
            iterator_to_array((new StepRunner())->steps(
                new PipelineSteps(),
                $extractor->extract(flow_context()->backend(), new Criteria()),
                flow_context(config())->withErrorHandler($handler),
                $extractor,
            )),
        );
        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(LoadingError::class, $handler->errors[0]);
        static::assertSame($rejects, $handler->errors[0]->loader);
    }

    public function test_a_run_that_routed_records_logs_how_many_and_where(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $extractor = from_array([
            ['id' => 'x'],
            ['id' => 2],
            ['id' => 'y'],
        ], schema(int_schema('id')))->withCorruptedRecords(new SpyLoader());

        iterator_to_array((new StepRunner())->steps(
            new PipelineSteps(),
            $extractor->extract(flow_context()->backend(), new Criteria()),
            $telemetry->flowContext,
            $extractor,
        ));

        static::assertCount(
            1,
            $telemetry->logs->entriesContaining('Routed 2 corrupted records to ' . SpyLoader::class . '.'),
        );
    }
}
