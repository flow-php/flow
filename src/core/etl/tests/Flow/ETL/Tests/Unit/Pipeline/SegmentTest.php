<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\ErrorHandler\ExtractionError;
use Flow\ETL\ErrorHandler\LoadingError;
use Flow\ETL\ErrorHandler\SkipRows;
use Flow\ETL\ErrorHandler\ThrowError;
use Flow\ETL\ErrorHandler\TransformationError;
use Flow\ETL\Pipeline\Segment;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\RecordingErrorHandler;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\Double\ThrowWhenRowMatches;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Transformer\LimitTransformer;
use Generator;
use RuntimeException;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class SegmentTest extends FlowTestCase
{
    public function test_a_segment_without_an_extractor_rethrows_its_input_error_untouched(): void
    {
        $handler = new RecordingErrorHandler();
        $failure = new RuntimeException('upstream failed');

        $thrown = null;

        try {
            iterator_to_array((new Segment())->execute(
                (static function () use ($failure): Generator {
                    yield RowsMother::sequentialIds(1);

                    throw $failure;
                })(),
                flow_context(config())->setErrorHandler($handler),
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
        $segment = new Segment(extractor: $extractor);
        $segment->add($loader);

        $yielded = iterator_to_array(
            $segment->execute(
                (static function (): Generator {
                    yield RowsMother::sequentialIds(2);

                    throw new RuntimeException('second batch failed');
                })(),
                flow_context(config())->setErrorHandler($handler),
            ),
            false,
        );

        static::assertSame(
            [[['id' => 1], ['id' => 2]]],
            array_map(static fn(Rows $rows): array => $rows->toArray(), $yielded),
        );
        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(ExtractionError::class, $handler->errors[0]);
        static::assertSame($extractor, $handler->errors[0]->extractor);
        static::assertSame('second batch failed', $handler->errors[0]->cause->getMessage());
        static::assertSame(1, $loader->closureCount);
    }

    public function test_extraction_error_propagates_under_throw_error(): void
    {
        $segment = new Segment(extractor: new CountingExtractor(schema(int_schema('id'))));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('source failed');

        iterator_to_array($segment->execute(
            (static function (): Generator {
                yield from [];

                throw new RuntimeException('source failed');
            })(),
            flow_context(config())->setErrorHandler(new ThrowError()),
        ));
    }

    public function test_extraction_error_reaches_the_handler(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('id')));
        $handler = new RecordingErrorHandler();
        $loader = new SpyLoader();
        $segment = new Segment(extractor: $extractor);
        $segment->add($loader);

        $yielded = iterator_to_array($segment->execute(
            (static function (): Generator {
                yield from [];

                throw new RuntimeException('source failed');
            })(),
            flow_context(config())->setErrorHandler($handler),
        ));

        static::assertSame([], $yielded);
        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(ExtractionError::class, $handler->errors[0]);
        static::assertSame($extractor, $handler->errors[0]->extractor);
        static::assertSame(1, $loader->closureCount);
    }

    public function test_ignore_error_drops_the_half_transformed_batch(): void
    {
        $handler = new RecordingErrorHandler();
        $segment = new Segment();
        $segment->add(new ThrowingTransformer(new RuntimeException('first')));
        $segment->add(new ThrowingTransformer(new RuntimeException('second')));

        static::assertSame(
            [],
            iterator_to_array($segment->execute(
                (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))->extract(
                    flow_context(),
                ),
                flow_context(config())->setErrorHandler($handler),
            )),
        );
        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(TransformationError::class, $handler->errors[0]);
        static::assertSame('first', $handler->errors[0]->cause->getMessage());
    }

    public function test_loading_error_skips_only_the_failing_loader(): void
    {
        $handler = new RecordingErrorHandler();
        $throwing = new ThrowingLoader(new RuntimeException('sink failed'));
        $spy = new SpyLoader();
        $segment = new Segment();
        $segment->add($throwing);
        $segment->add($spy);

        iterator_to_array($segment->execute(
            (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))->extract(flow_context()),
            flow_context(config())->setErrorHandler($handler),
        ));

        static::assertSame(1, $throwing->loadsCount);
        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], $spy->loadedRowsToArray());
        static::assertCount(1, $handler->errors);
        static::assertInstanceOf(LoadingError::class, $handler->errors[0]);
        static::assertSame($throwing, $handler->errors[0]->loader);
    }

    public function test_skip_rows_drops_the_batch_and_continues_with_the_next(): void
    {
        $segment = new Segment();
        $segment->add(new ThrowWhenRowMatches('id', 2, new RuntimeException('boom')));

        static::assertSame(
            [['id' => 1], ['id' => 3]],
            array_merge(...array_map(
                static fn(Rows $rows): array => $rows->toArray(),
                iterator_to_array(
                    $segment->execute(
                        (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))
                            ->withBatchSize(1)
                            ->extract(flow_context()),
                        flow_context(config())->setErrorHandler(new SkipRows()),
                    ),
                    false,
                ),
            )),
        );
    }

    public function test_stop_is_forwarded_upstream_without_pulling_another_batch(): void
    {
        $extractor = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(3)))->withBatchSize(1);
        $segment = new Segment();
        $segment->add(new LimitTransformer(1));

        static::assertCount(1, iterator_to_array(
            $segment->execute($extractor->extract(flow_context()), flow_context(config())),
            false,
        ));
        static::assertSame(1, $extractor->batchesYielded);
    }

    public function test_a_propagated_extraction_error_is_logged(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $segment = new Segment(extractor: new CountingExtractor(schema(int_schema('id'))));

        $thrown = null;

        try {
            iterator_to_array($segment->execute(
                (static function (): Generator {
                    yield from [];

                    throw new RuntimeException('source failed');
                })(),
                $telemetry->flowContext,
            ));
        } catch (RuntimeException $e) {
            $thrown = $e;
        }

        static::assertInstanceOf(RuntimeException::class, $thrown);
        static::assertCount(1, $telemetry->logs->entriesContaining('Error during extraction.'));
    }
}
