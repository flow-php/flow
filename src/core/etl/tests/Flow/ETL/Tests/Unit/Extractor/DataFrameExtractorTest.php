<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\ErrorHandler\ExtractionError;
use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Extractor\DataFrameExtractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\RecordingErrorHandler;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\Double\RepeatableExtractor;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\Double\UndescribableRowLessExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use RuntimeException;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class DataFrameExtractorTest extends FlowTestCase
{
    public function test_a_build_error_in_the_wrapped_frame_is_not_a_schema_refusal(): void
    {
        $extractor = from_data_frame(
            df()->read(from_rows(rows(schema(int_schema('id')), row(['id' => 1]))))->select('nope'),
        );

        try {
            $extractor->schema();

            static::fail('Expected the wrapped frame\'s build error to reach the caller.');
        } catch (InvalidArgumentException $e) {
            static::assertInstanceOf(SchemaDefinitionNotFoundException::class, $e);
            static::assertNotInstanceOf(SchemaNotDerivableException::class, $e);
        }
    }

    public function test_a_declared_schema_wins_over_the_wrapped_frame(): void
    {
        $extractor = from_data_frame(df()->read(from_rows(rows(
            schema(int_schema('id')),
            row(['id' => 1]),
            row(['id' => 2]),
        ))))
            ->withSchema(schema(str_schema('id')));

        $batches = iterator_to_array($extractor->extract(flow_context(config())));

        static::assertEquals(schema(str_schema('id')), $extractor->schema());
        static::assertEquals(schema(str_schema('id')), $batches[0]->schema());
        static::assertSame([['id' => '1'], ['id' => '2']], $batches[0]->toArray());
    }

    public function test_a_declared_schema_describes_a_frame_that_cannot_describe_itself(): void
    {
        $declared = schema(int_schema('id'));

        static::assertEquals(
            $declared,
            from_data_frame(df()->read(new UndescribableRowLessExtractor()))->withSchema($declared)->schema(),
        );
    }

    /**
     * @return Generator<string, array{bool}>
     */
    public static function declared_schemas(): Generator
    {
        yield 'derived schema' => [false];
        yield 'declared schema' => [true];
    }

    #[DataProvider('declared_schemas')]
    public function test_a_failure_inside_the_frame_is_offered_to_the_outer_handler_as_an_extraction_error(bool $declared): void
    {
        $failure = new RuntimeException('boom');
        $extractor = from_data_frame(
            df()->read(from_array([['id' => 1], ['id' => 2]]))->transform(new ThrowingTransformer($failure)),
        );

        if ($declared) {
            $extractor->withSchema(schema(int_schema('id')));
        }

        $handler = new RecordingErrorHandler(new IgnoreError());

        $rows = df()->read($extractor)->onError($handler)->fetch();

        static::assertSame(0, $rows->count());
        static::assertCount(1, $handler->errors);
        $error = $handler->errors[0];
        static::assertInstanceOf(ExtractionError::class, $error);
        static::assertSame($failure, $error->cause);
        static::assertSame($extractor, $error->extractor);
    }

    public function test_a_failure_inside_a_wrapped_frame_is_offered_to_the_outer_handler_as_an_extraction_error(): void
    {
        $failure = new RuntimeException('boom');
        $chain = from_all(from_data_frame(
            df()->read(from_array([['id' => 1], ['id' => 2]]))->transform(new ThrowingTransformer($failure)),
        ));
        $handler = new RecordingErrorHandler(new IgnoreError());

        $rows = df()->read($chain)->onError($handler)->fetch();

        static::assertSame(0, $rows->count());
        static::assertCount(1, $handler->errors);
        $error = $handler->errors[0];
        static::assertInstanceOf(ExtractionError::class, $error);
        static::assertSame($failure, $error->cause);
        static::assertSame($chain, $error->extractor);
    }

    public function test_it_repeats_when_every_source_of_its_frame_repeats(): void
    {
        static::assertTrue(from_data_frame(df()->read(from_array([['id' => 1]])))->isRepeatable());
        static::assertFalse(from_data_frame(df()->read(new RepeatableExtractor(false)))->isRepeatable());
    }

    public function test_a_limit_given_to_extract_reaches_the_frames_source(): void
    {
        $source = new RecordingFileExtractor(
            schema(int_schema('id')),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3])),
        );

        $batches = iterator_to_array(from_data_frame(df()->read($source))->extract(flow_context(config()), 2), false);

        static::assertSame([2], $source->limits);
        static::assertSame(
            [['id' => 1], ['id' => 2]],
            array_merge(...array_map(static fn($rows) => $rows->toArray(), $batches)),
        );
    }

    public function test_a_refusal_from_the_wrapped_frame_is_re_raised_unchanged(): void
    {
        $extractor = from_data_frame(df()->read(new UndescribableRowLessExtractor()));

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage(
            UndescribableRowLessExtractor::class
            . ' cannot describe what it will produce before producing it. '
            . 'Declare the schema with ->withSchema(), or read from a source that describes itself.',
        );

        $extractor->schema();
    }

    public function test_the_frame_is_frozen_at_construction(): void
    {
        $inner = df()->read(from_rows(rows(schema(int_schema('id')), row(['id' => 1]))));

        $extractor = from_data_frame($inner);
        $inner->withEntry('doubled', ref('id')->multiply(lit(2)));

        static::assertSame([['id' => 1]], iterator_to_array($extractor->extract(flow_context(config())))[0]->toArray());
    }

    public function test_it_is_constructed_from_a_frame(): void
    {
        static::assertSame(
            [['id' => 1]],
            df()
                ->read(new DataFrameExtractor(df()->read(from_rows(rows(schema(int_schema('id')), row(['id' => 1]))))))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_an_error_handler_set_after_embedding_does_not_reach_the_frame(): void
    {
        $inner = df()
            ->read(from_array([['id' => 1]]))
            ->with(new ThrowingTransformer(new RuntimeException('inner boom')));
        $outer = df()->read(from_data_frame($inner));
        $inner->onError(new IgnoreError());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('inner boom');

        $outer->fetch();
    }

    public function test_extract_runs_the_snapshot_plan_when_reached_through_a_chain_wrapper(): void
    {
        $inner = df()->read(from_rows(rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]))))->limit(1);

        static::assertSame(
            [['id' => 1], ['id' => 3]],
            df()
                ->read(from_all(from_data_frame($inner), from_array([['id' => 3]])))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_extract_stops_on_signal_stop(): void
    {
        $counting = new CountingExtractor(
            schema(int_schema('id')),
            rows(schema(int_schema('id')), row(['id' => 1])),
            rows(schema(int_schema('id')), row(['id' => 2])),
        );
        $counting->withBatchSize(1);
        $generator = from_data_frame(df()->read($counting))->extract(flow_context(config()));

        $generator->current();
        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
        static::assertSame(1, $counting->batchesYielded);
    }

    public function test_the_snapshot_source_is_read_once_across_schema_and_extract(): void
    {
        $counting = new CountingExtractor(schema(int_schema('id')), rows(schema(int_schema('id')), row(['id' => 1])));
        $extractor = from_data_frame(df()->read($counting)->limit(1));

        $extractor->schema();
        $batches = iterator_to_array($extractor->extract(flow_context(config())));

        static::assertCount(1, $batches);
        static::assertSame(1, $counting->extractCalls);
        static::assertSame([['id' => 1]], $batches[0]->toArray());
    }

    public function test_extract_opens_and_closes_one_dataframe_span_for_the_wrapped_frame(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $extractor = from_data_frame(
            df($telemetry->config)->read(from_rows(rows(schema(int_schema('id')), row(['id' => 1])))),
        )
            ->withSchema(schema(int_schema('id')));

        iterator_to_array($extractor->extract(flow_context(config())));

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
    }

    public function test_extracting_from_another_data_frame(): void
    {
        $extractor = from_data_frame(df()->read(from_rows(
            rows(schema(str_schema('value')), row(['value' => 'test']), row(['value' => 'test'])),
            rows(schema(str_schema('value')), row(['value' => 'test']), row(['value' => 'test'])),
        )));

        self::assertExtractedRowsEquals(
            rows(
                schema(str_schema('value')),
                row(['value' => 'test']),
                row(['value' => 'test']),
                row(['value' => 'test']),
                row(['value' => 'test']),
            ),
            $extractor,
        );
    }

    public function test_a_limited_wrapped_frame_extracted_twice_is_planned_per_run(): void
    {
        $extractor = from_all(
            from_data_frame(df()->read(from_array([['id' => 1], ['id' => 2]]))->limit(1)),
            from_array([['id' => 3]]),
        );

        $first = [];

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $first = [...$first, ...$rows->toArray()];
        }

        $second = [];

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $second = [...$second, ...$rows->toArray()];
        }

        static::assertSame([['id' => 1], ['id' => 3]], $first);
        static::assertSame([['id' => 1], ['id' => 3]], $second);
    }

    public function test_it_describes_the_wrapped_frame_without_reading_it(): void
    {
        $counting = new CountingExtractor(schema(int_schema('id'), str_schema('name')));

        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            from_data_frame(df()->read($counting))->schema(),
        );
        static::assertSame(0, $counting->extractCalls);
    }
}
