<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Executor;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Scan;
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Plan\Pipeline;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\RecordingScanExtractor;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use RuntimeException;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
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

        $batches = iterator_to_array((new Executor())->execute(new Pipeline(0, $segments, NodeMother::context())));

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

        $batches = iterator_to_array((new Executor())->execute(
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

        $batches = iterator_to_array((new Executor())->execute(
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

        iterator_to_array((new Executor())->execute(
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

        iterator_to_array((new Executor())->execute(
            new Pipeline(1, new Segments(), NodeMother::context(config()), new Pipeline(0, $upstream, $leafContext)),
        ));

        static::assertSame([$leafContext], $extractor->contexts);
    }

    public function test_a_pipeline_without_a_source_throws(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('pipeline #7 has no source extractor');

        iterator_to_array((new Executor())->execute(new Pipeline(7, new Segments(), NodeMother::context())));
    }

    public function test_a_scannable_source_receives_the_pipelines_scan(): void
    {
        $extractor = new RecordingScanExtractor(schema(int_schema('id')), RowsMother::sequentialIds(1));
        $scan = new Scan(limit: 7);

        iterator_to_array((new Executor())->execute(
            new Pipeline(0, new Segments($extractor), NodeMother::context(), scan: $scan),
        ));

        static::assertSame([$scan], $extractor->scans);
    }

    public function test_an_inlined_stage_opens_and_closes_its_own_frames_span(): void
    {
        $inner = new MemoryTelemetryContext();
        $upstream = new Segments(from_rows(RowsMother::sequentialIds(1)));
        $upstream->add(new BatchingProcessor(1));

        iterator_to_array((new Executor())->execute(
            new Pipeline(
                1,
                new Segments(),
                NodeMother::context(config()),
                new Pipeline(0, $upstream, $inner->flowContext),
            ),
        ));

        static::assertCount(1, $inner->spans->startedSpans());
        static::assertCount(1, $inner->spans->endedSpans());
    }

    public function test_an_abandoned_inlined_stage_closes_its_frames_span(): void
    {
        $inner = new MemoryTelemetryContext();
        $upstream = new Segments(from_rows(RowsMother::sequentialIds(2)));
        $upstream->add(new BatchingProcessor(1));
        $generator = (new Executor())->execute(
            new Pipeline(
                1,
                new Segments(),
                NodeMother::context(config()),
                new Pipeline(0, $upstream, $inner->flowContext),
            ),
        );

        $generator->current();
        unset($generator);

        static::assertCount(1, $inner->spans->startedSpans());
        static::assertCount(1, $inner->spans->endedSpans());
        static::assertNotTrue($inner->spans->endedSpans()[0]->status()?->isError());
    }

    public function test_a_failure_below_an_inlined_stage_closes_its_frames_span_as_failed(): void
    {
        $inner = new MemoryTelemetryContext();
        $failure = new RuntimeException('stage exploded');
        $upstream = new Segments(from_rows(RowsMother::sequentialIds(1)));
        $upstream->add(new BatchingProcessor(1));
        $downstream = new Segments();
        $downstream->add(new ThrowingTransformer($failure));

        try {
            iterator_to_array((new Executor())->execute(
                new Pipeline(
                    1,
                    $downstream,
                    NodeMother::context(config()),
                    new Pipeline(0, $upstream, $inner->flowContext),
                ),
            ));

            static::fail('Expected the failure to be rethrown.');
        } catch (RuntimeException $e) {
            static::assertSame($failure, $e);
        }

        static::assertCount(1, $inner->spans->endedSpans());
        static::assertTrue($inner->spans->endedSpans()[0]->status()?->isError());
    }
}
