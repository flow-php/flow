<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use ArrayObject;
use Exception;
use Flow\ETL\Foundation\Clock\FakeClock;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\StageProgress;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Operators\Join\CrossJoin;
use Flow\ETL\Operators\Processor;
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
use Flow\ETL\Runtime\Executor\StepTelemetry;
use Flow\ETL\Runtime\Telemetry\TelemetryOptions;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Context\VmStackContext;
use Flow\ETL\Tests\Double\ClosureThrowingLoader;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\FailsWhen;
use Flow\ETL\Tests\Double\FrameRecordingProcessor;
use Flow\ETL\Tests\Double\RecordingErrorHandler;
use Flow\ETL\Tests\Double\RecordingHeartbeat;
use Flow\ETL\Tests\Double\RecordingLoader;
use Flow\ETL\Tests\Double\RecordingProcessor;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\SwallowingProcessor;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use Flow\ETL\Tests\Double\TracedFailureTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Telemetry\Logger\LogEntry;
use Generator;
use LogicException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\Attributes\TestWith;
use RuntimeException;
use SensitiveParameter;

use function array_map;
use function array_merge;
use function array_slice;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\to_floe;
use function ini_set;
use function iterator_to_array;

final class StepRunnerTest extends FlowTestCase
{
    public function test_unwinding_an_input_that_rethrows_surfaces_the_same_failure(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $ended = new ArrayObject();
        /** @var Generator<int, Rows, null|Signal, void> $input */
        $input = (static function () use ($ended): Generator {
            try {
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
            } catch (RuntimeException $failure) {
                throw $failure;
            } finally {
                $ended->append('finally');
            }
        })();
        $input->current();

        (new StepRunner())->unwind($input, new RuntimeException('failed'), $telemetry->flowContext);

        static::assertSame([['finally'], 0], [
            $ended->getArrayCopy(),
            count($telemetry->logs->entriesContaining('The input of a failed run failed to end')),
        ]);
    }

    public function test_unwinding_an_input_that_swallows_the_failure_logs_nothing(): void
    {
        $telemetry = new MemoryTelemetryContext();
        /** @var Generator<int, Rows, null|Signal, void> $input */
        $input = (static function (): Generator {
            try {
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
            } catch (RuntimeException) {
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
            }
        })();
        $input->current();

        (new StepRunner())->unwind($input, new RuntimeException('failed'), $telemetry->flowContext);

        static::assertSame([], $telemetry->logs->entriesContaining('The input of a failed run failed to end'));
    }

    public function test_unwinding_an_input_that_replaces_the_failure_logs_the_replacement(): void
    {
        $telemetry = new MemoryTelemetryContext();
        /** @var Generator<int, Rows, null|Signal, void> $input */
        $input = (static function (): Generator {
            try {
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
            } catch (RuntimeException) {
                throw new LogicException('cleanup failed');
            }
        })();
        $input->current();

        (new StepRunner())->unwind($input, new RuntimeException('failed'), $telemetry->flowContext);

        static::assertCount(1, $telemetry->logs->entriesContaining('The input of a failed run failed to end'));
    }

    public function test_unwinding_a_finished_input_leaves_the_failure_as_it_is(): void
    {
        $telemetry = new MemoryTelemetryContext();
        /** @var Generator<int, Rows, null|Signal, void> $input */
        $input = (static function (): Generator {
            yield array_to_rows([['id' => 1]], schema(int_schema('id')));
        })();
        iterator_to_array($input);
        $failure = new RuntimeException('failed');

        (new StepRunner())->unwind($input, $failure, $telemetry->flowContext);

        static::assertSame([null, []], [
            $failure->getPrevious(),
            $telemetry->logs->entriesContaining('The input of a failed run failed to end'),
        ]);
    }

    public function test_unwinding_an_aborted_input_leaves_the_failure_as_it_is(): void
    {
        $telemetry = new MemoryTelemetryContext();
        /** @var Generator<int, Rows, null|Signal, void> $input */
        $input = (static function (): Generator {
            yield array_to_rows([['id' => 1]], schema(int_schema('id')));

            throw new LogicException('source aborted');
        })();
        $input->current();

        try {
            $input->next();
        } catch (LogicException) {
        }

        $failure = new RuntimeException('failed');

        (new StepRunner())->unwind($input, $failure, $telemetry->flowContext);

        static::assertSame([null, []], [
            $failure->getPrevious(),
            $telemetry->logs->entriesContaining('The input of a failed run failed to end'),
        ]);
    }

    public function test_unwinding_a_traced_operator_ends_the_processor_it_holds(): void
    {
        $telemetry = new MemoryTelemetryContext(new TelemetryOptions(traceTransformations: true));
        $ended = new ArrayObject();
        /** @var Generator<int, Rows, null|Signal, void> $processor */
        $processor = (static function () use ($ended): Generator {
            try {
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
            } finally {
                $ended->append('processor finally');
            }
        })();
        $operator = (new StepTelemetry())->operator(VoidProcessor::class, $processor, $telemetry->flowContext);
        unset($processor);
        $operator->current();
        (new StepRunner())->unwind($operator, new RuntimeException('failed'), $telemetry->flowContext);

        static::assertSame(['processor finally'], $ended->getArrayCopy());
    }

    #[TestWith([true])]
    #[TestWith([false])]
    public function test_a_failed_run_throws_its_failure_into_a_processor_before_it_ends_the_source(bool $traced): void
    {
        $log = new ArrayObject();
        $steps = (new StepRunner())->steps(
            new PipelineSteps([new RecordingProcessor($log), new TracedFailureTransformer()]),
            (static function () use ($log): Generator {
                try {
                    yield RowsMother::sequentialIds(1);
                    yield RowsMother::sequentialIds(1);
                } finally {
                    $log->append('source finally');
                }
            })(),
            (new MemoryTelemetryContext(new TelemetryOptions(traceTransformations: $traced)))->flowContext,
        );

        try {
            iterator_to_array($steps);

            static::fail('Expected the failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame('step exploded', $e->getMessage());
            static::assertSame(
                ['processor caught step exploded', 'processor finally', 'source finally'],
                $log->getArrayCopy(),
            );
        }
    }

    public function test_unwinding_an_operator_ends_the_chain_it_holds_in_a_local(): void
    {
        $ended = new ArrayObject();
        /** @var Generator<int, Rows, null|Signal, void> $operator */
        $operator = (static function () use ($ended): Generator {
            try {
                $joined = (
                    /**
                     * @param Generator<int, Rows> $left
                     *
                     * @return Generator<int, Rows>
                     */
                    static function (Generator $left): Generator {
                        foreach ($left as $rows) {
                            yield $rows;
                        }
                    }
                )(
                    (static function () use ($ended): Generator {
                        try {
                            yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                            yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                        } finally {
                            $ended->append('reader closed');
                        }
                    })(),
                );

                foreach ($joined as $rows) {
                    yield $rows;
                }
            } catch (LogicException $other) {
                throw new RuntimeException('wrapped', 0, $other);
            } finally {
                $ended->append('buckets cleared');
            }
        })();
        $operator->current();
        (new StepRunner())->unwind($operator, new RuntimeException('failed'), flow_context());

        static::assertSame(['buckets cleared', 'reader closed'], $ended->getArrayCopy());
    }

    public function test_unwinding_a_processor_ends_the_input_it_owns(): void
    {
        $ended = new ArrayObject();
        /** @var Generator<int, Rows, null|Signal, void> $processor */
        $processor = (
            /**
             * @param Generator<int, Rows> $rows
             *
             * @return Generator<int, Rows>
             */
            static function (Generator $rows): Generator {
                foreach ($rows as $batch) {
                    yield $batch;
                }
            }
        )(
            (static function () use ($ended): Generator {
                try {
                    yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                } finally {
                    $ended->append('input finally');
                }
            })(),
        );
        $processor->current();
        (new StepRunner())->unwind($processor, new RuntimeException('failed'), flow_context());

        static::assertSame(['input finally'], $ended->getArrayCopy());
    }

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

    public function test_a_failing_closure_still_closes_the_loaders_after_it(): void
    {
        $failure = new RuntimeException('closure failed');
        $failing = new ClosureThrowingLoader($failure);
        $next = new RecordingLoader();

        static::assertSame([$failure], (new StepRunner())->endLoaders([$failing, $next], flow_context(config()), true));
        static::assertSame(1, $failing->discarded);
        static::assertSame(['closure'], $next->log);
    }

    public function test_a_commit_entry_has_the_location_of_its_write(): void
    {
        $recording = new RecordingHeartbeat();
        $context = flow_context(
            config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60)->build(),
        );
        $memory = memory_filesystem();
        $loaders = [
            to_floe(path('memory://a/part-{part}.floe'), filesystem: $memory),
            to_floe(path('memory://b/rows.floe'), filesystem: $memory),
        ];
        $context->config->heartbeat()?->started();

        foreach ($loaders as $loader) {
            $loader->load(RowsMother::sequentialIds(2), $context->backend());
        }

        (new StepRunner())->endLoaders($loaders, $context, true);
        $context->config->heartbeat()?->completed();

        static::assertSame(
            ['memory://a/', 'memory://b/'],
            array_map(
                static fn(StageProgress $commit): ?string => $commit->subject,
                HeartbeatContext::entries($recording, StageName::commit)[0],
            ),
        );
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

    public function test_end_loaders_is_done_after_every_loader(): void
    {
        $recording = new RecordingHeartbeat();
        $context = flow_context(
            config_builder()->clock(new FakeClock())->heartbeat($recording, everySeconds: 60)->build(),
        );
        $memory = memory_filesystem();
        $loaders = [
            to_floe(path('memory://a/part-{part}.floe'), filesystem: $memory),
            to_floe(path('memory://b/rows.floe'), filesystem: $memory),
        ];
        $context->config->heartbeat()?->started();

        foreach ($loaders as $loader) {
            $loader->load(RowsMother::sequentialIds(2), $context->backend());
        }

        (new StepRunner())->endLoaders($loaders, $context, true);
        $context->config->heartbeat()?->beatNow();

        static::assertSame(
            [true, true],
            array_map(
                static fn(StageProgress $commit): bool => $commit->endedAtSeconds !== null,
                HeartbeatContext::entries($recording, StageName::commit)[0],
            ),
        );
    }

    #[RunInSeparateProcess]
    #[TestWith(['0'])]
    #[TestWith(['1'])]
    public function test_a_processor_that_fails_in_its_own_body_fails_the_run_under_user_code(string $ignoreArgs): void
    {
        ini_set('zend.exception_ignore_args', $ignoreArgs);
        $processor = new FrameRecordingProcessor(FailsWhen::secondBatch);
        $steps = (new StepRunner())->steps(
            new PipelineSteps([$processor]),
            (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))
                ->withBatchSize(1)
                ->extract(flow_context()->backend(), new Criteria()),
            flow_context(config()),
        );
        VmStackContext::fillWithInts();

        try {
            iterator_to_array($steps, false);

            static::fail('Expected the processor failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame('processor failed', $e->getMessage());
        }

        static::assertSame([true, true], [$processor->failedUnderUserCode, $processor->heldEnded]);
    }

    #[RunInSeparateProcess]
    #[TestWith(['0'])]
    #[TestWith(['1'])]
    public function test_a_traced_processor_that_fails_in_its_own_body_fails_the_run_under_user_code(string $ignoreArgs): void
    {
        ini_set('zend.exception_ignore_args', $ignoreArgs);
        $processor = new FrameRecordingProcessor(FailsWhen::secondBatch);
        $steps = (new StepRunner())->steps(
            new PipelineSteps([$processor]),
            (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))
                ->withBatchSize(1)
                ->extract(flow_context()->backend(), new Criteria()),
            (new MemoryTelemetryContext(telemetry_options(trace_transformations: true)))->flowContext,
        );
        VmStackContext::fillWithInts();

        try {
            iterator_to_array($steps, false);

            static::fail('Expected the processor failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame('processor failed', $e->getMessage());
        }

        static::assertSame([true, true], [$processor->failedUnderUserCode, $processor->heldEnded]);
    }

    #[RunInSeparateProcess]
    #[TestWith(['0'])]
    #[TestWith(['1'])]
    public function test_a_processor_that_fails_on_stop_fails_the_run_under_user_code(string $ignoreArgs): void
    {
        ini_set('zend.exception_ignore_args', $ignoreArgs);
        $processor = new FrameRecordingProcessor(FailsWhen::stop);
        $steps = (new StepRunner())->steps(
            new PipelineSteps([$processor, new LimitTransformer(1)]),
            (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))
                ->withBatchSize(1)
                ->extract(flow_context()->backend(), new Criteria()),
            flow_context(config()),
        );
        VmStackContext::fillWithInts();

        try {
            iterator_to_array($steps, false);

            static::fail('Expected the processor failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame('processor failed on stop', $e->getMessage());
        }

        static::assertSame([true, true], [$processor->failedUnderUserCode, $processor->heldEnded]);
    }

    #[RunInSeparateProcess]
    #[TestWith(['0'])]
    #[TestWith(['1'])]
    public function test_a_processor_that_swallows_the_failure_is_logged(string $ignoreArgs): void
    {
        ini_set('zend.exception_ignore_args', $ignoreArgs);
        $telemetry = new MemoryTelemetryContext();
        $steps = (new StepRunner())->steps(
            new PipelineSteps([new SwallowingProcessor(), new ThrowingLoader(new RuntimeException('write failed'))]),
            (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))
                ->withBatchSize(1)
                ->extract(flow_context()->backend(), new Criteria()),
            $telemetry->flowContext,
        );
        VmStackContext::fillWithInts();

        try {
            iterator_to_array($steps, false);

            static::fail('Expected the loader failure to surface.');
        } catch (RuntimeException $e) {
            static::assertSame('write failed', $e->getMessage());
        }

        $warnings = $telemetry->logs->entriesContaining('kept running after the failure');
        static::assertCount(1, $warnings);
        static::assertStringContainsString(SwallowingProcessor::class . '::process()', $warnings[0]->record->body);
    }

    #[RunInSeparateProcess]
    #[TestWith(['0'])]
    #[TestWith(['1'])]
    public function test_a_process_that_returns_a_generator_past_its_first_yield_fails_with_a_rewind_error(string $ignoreArgs): void
    {
        ini_set('zend.exception_ignore_args', $ignoreArgs);
        $steps = (new StepRunner())->steps(
            new PipelineSteps([new class() implements Processor {
                public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
                {
                    throw InvalidArgumentException::because('%s holds live state and is never described', self::class);
                }

                public function describe(): ?Description
                {
                    return null;
                }

                public function bind(Schema $input): BoundStep
                {
                    return new BoundStep($this, $input);
                }

                public function breaks(): bool
                {
                    return false;
                }

                public function process(Generator $rows, Backend $backend): Generator
                {
                    $batches = (static function () use ($rows): Generator {
                        yield from $rows;
                    })();
                    $batches->current();
                    $batches->next();

                    return $batches;
                }
            }]),
            (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))
                ->withBatchSize(1)
                ->extract(flow_context()->backend(), new Criteria()),
            flow_context(config()),
        );
        VmStackContext::fillWithInts();

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Cannot rewind a generator that was already run');

        iterator_to_array($steps, false);
    }

    public function test_a_stop_reaches_a_traced_operator(): void
    {
        $received = new ArrayObject();

        iterator_to_array(
            (new StepRunner())->steps(
                new PipelineSteps([
                    new class($received) implements Processor {
                        /**
                         * @param ArrayObject<int, mixed> $received
                         */
                        public function __construct(
                            private readonly ArrayObject $received,
                        ) {}

                        public static function fromDescription(
                            #[SensitiveParameter]
                            array $data,
                            Rebuild $rebuild,
                        ): static {
                            throw InvalidArgumentException::because(
                                '%s holds live state and is never described',
                                self::class,
                            );
                        }

                        public function describe(): ?Description
                        {
                            return null;
                        }

                        public function bind(Schema $input): BoundStep
                        {
                            return new BoundStep($this, $input);
                        }

                        public function breaks(): bool
                        {
                            return false;
                        }

                        /**
                         * @param Generator<Rows> $rows
                         *
                         * @return Generator<int, Rows, null|Signal, void>
                         */
                        public function process(Generator $rows, Backend $backend): Generator
                        {
                            foreach ($rows as $batch) {
                                $signal = yield $batch;
                                $this->received[] = $signal;

                                if ($signal === Signal::STOP) {
                                    return;
                                }
                            }
                        }
                    },
                    new LimitTransformer(1),
                ]),
                (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))
                    ->withBatchSize(1)
                    ->extract(flow_context()->backend(), new Criteria()),
                (new MemoryTelemetryContext(telemetry_options(trace_transformations: true)))->flowContext,
            ),
            false,
        );

        static::assertSame([Signal::STOP], $received->getArrayCopy());
    }
}
