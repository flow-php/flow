<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Plan\Described;
use Flow\ETL\Plan\FrameOutput;
use Flow\ETL\Plan\Pipeline;
use Flow\ETL\Plan\Raw;
use Flow\ETL\Plan\Refusal;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Tests\Mother\RunMother;
use Flow\ETL\Transformer\RenameEntryTransformer;
use RuntimeException;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class FrameOutputTest extends FlowTestCase
{
    public function test_get_executes_the_pipeline(): void
    {
        $segments = new Segments(from_rows(RowsMother::sequentialIds(3)));
        $segments->add(new BatchingProcessor(2));
        $segments->add(new RenameEntryTransformer('id', 'b'));

        $batches = iterator_to_array(
            (new FrameOutput(
                new Described(new Pipeline(0, $segments, NodeMother::context()), schema(int_schema('b'))),
                RunMother::default(),
            ))->get(),
        );

        static::assertCount(2, $batches);
        static::assertSame([['b' => 1], ['b' => 2]], $batches[0]->toArray());
    }

    public function test_fetch_merges_every_batch_into_one_rows(): void
    {
        $segments = new Segments(from_rows(RowsMother::sequentialIds(3)));
        $segments->add(new BatchingProcessor(1));

        $rows = (new FrameOutput(
            new Described(new Pipeline(0, $segments, NodeMother::context()), schema(int_schema('id'))),
            RunMother::default(),
        ))->fetch();

        static::assertSame([['id' => 1], ['id' => 2], ['id' => 3]], $rows->toArray());
    }

    public function test_fetch_of_an_empty_pipeline_returns_rows_with_the_output_schema(): void
    {
        $rows = (new FrameOutput(
            new Described(
                new Pipeline(0, new Segments(from_rows(rows(schema(int_schema('id'))))), NodeMother::context()),
                schema(int_schema('id')),
            ),
            RunMother::default(),
        ))->fetch();

        static::assertSame(0, $rows->count());
        static::assertEquals(schema(int_schema('id')), $rows->schema());
    }

    public function test_fetch_of_an_empty_refused_pipeline_returns_rows_with_an_empty_schema(): void
    {
        $rows = (new FrameOutput(
            new Raw(
                new Pipeline(0, new Segments(from_rows(rows(schema(int_schema('id'))))), NodeMother::context()),
                Refusal::of(SchemaNotDerivableException::extractor('x')),
            ),
            RunMother::default(),
        ))->fetch();

        static::assertSame(0, $rows->count());
        static::assertEquals(schema(), $rows->schema());
    }

    public function test_schema_is_the_described_plans_schema(): void
    {
        $schema = schema(int_schema('id'));

        static::assertSame(
            $schema,
            (new FrameOutput(
                new Described(
                    new Pipeline(0, new Segments(from_rows(RowsMother::sequentialIds(1))), NodeMother::context()),
                    $schema,
                ),
                RunMother::default(),
            ))->schema(),
        );
    }

    public function test_schema_throws_the_rebuilt_refusal(): void
    {
        $output = new FrameOutput(
            new Raw(
                new Pipeline(0, new Segments(from_rows(RowsMother::sequentialIds(1))), NodeMother::context()),
                Refusal::of(SchemaNotDerivableException::extractor('x', 'no header')),
            ),
            RunMother::default(),
        );

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('x cannot describe what it will produce before producing it: no header. Declare the schema with '
        . '->withSchema().');

        $output->schema();
    }

    public function test_get_opens_and_closes_the_frames_own_dataframe_span(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $segments = new Segments(from_rows(RowsMother::sequentialIds(1)));

        iterator_to_array(
            (new FrameOutput(
                new Described(new Pipeline(0, $segments, $telemetry->flowContext), schema(int_schema('id'))),
                RunMother::default(),
            ))->get(),
        );

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
    }

    public function test_a_failing_frame_closes_its_span_as_failed_and_rethrows(): void
    {
        $telemetry = new MemoryTelemetryContext();
        $failure = new RuntimeException('right side exploded');
        $segments = new Segments(from_rows(RowsMother::sequentialIds(1)));
        $segments->add(new ThrowingTransformer($failure));
        $output = new FrameOutput(
            new Described(new Pipeline(0, $segments, $telemetry->flowContext), schema(int_schema('id'))),
            RunMother::default(),
        );

        try {
            $output->fetch();

            static::fail('Expected the frame failure to be rethrown.');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }
}
