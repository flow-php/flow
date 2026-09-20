<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\Executor\Segments;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Tests\Context\PipelineSteps;
use Flow\ETL\Tests\Double\PassThroughProcessor;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class SegmentsTest extends FlowTestCase
{
    public function test_add_loader_goes_into_the_only_segment(): void
    {
        $segments = new Segments();
        $loader = new SpyLoader();

        $segments->add($loader);

        static::assertCount(1, $segments->all());
        static::assertSame([$loader], $segments->all()[0]->steps());
    }

    public function test_add_multiple_transformers_and_loaders_go_into_the_only_segment(): void
    {
        $segments = new Segments();
        $transformer1 = new SpyTransformer();
        $transformer2 = new SpyTransformer();
        $loader = new SpyLoader();

        $segments->add($transformer1);
        $segments->add($transformer2);
        $segments->add($loader);

        static::assertCount(1, $segments->all());
        static::assertSame([$transformer1, $transformer2, $loader], $segments->all()[0]->steps());
    }

    public function test_add_processor_creates_new_segment(): void
    {
        $segments = new Segments();
        $transformer = new SpyTransformer();
        $processor = new PassThroughProcessor();

        $segments->add($transformer);
        $segments->add($processor);

        $allSegments = $segments->all();

        static::assertCount(2, $allSegments);
        static::assertSame([$transformer], $allSegments[0]->steps());
        static::assertSame($processor, $allSegments[0]->processor());
        static::assertSame([], $allSegments[1]->steps());
        static::assertNull($allSegments[1]->processor());
    }

    public function test_add_processor_with_empty_steps_creates_segment_with_processor(): void
    {
        $segments = new Segments();
        $processor = new PassThroughProcessor();

        $segments->add($processor);

        $allSegments = $segments->all();

        static::assertCount(2, $allSegments);
        static::assertSame([], $allSegments[0]->steps());
        static::assertSame($processor, $allSegments[0]->processor());
    }

    public function test_add_transformer_goes_into_the_only_segment(): void
    {
        $segments = new Segments();
        $transformer = new SpyTransformer();

        $segments->add($transformer);

        static::assertCount(1, $segments->all());
        static::assertSame([$transformer], $segments->all()[0]->steps());
    }

    public function test_all_returns_all_segments_including_current(): void
    {
        $segments = new Segments();
        $transformer1 = new SpyTransformer();
        $processor = new PassThroughProcessor();
        $transformer2 = new SpyTransformer();

        $segments->add($transformer1);
        $segments->add($processor);
        $segments->add($transformer2);

        $allSegments = $segments->all();

        static::assertCount(2, $allSegments);
        static::assertSame([$transformer1], $allSegments[0]->steps());
        static::assertSame($processor, $allSegments[0]->processor());
        static::assertSame([$transformer2], $allSegments[1]->steps());
        static::assertNull($allSegments[1]->processor());
    }

    public function test_multiple_processors_create_multiple_segments(): void
    {
        $segments = new Segments();
        $transformer1 = new SpyTransformer();
        $processor1 = new PassThroughProcessor();
        $transformer2 = new SpyTransformer();
        $processor2 = new PassThroughProcessor();
        $loader = new SpyLoader();

        $segments->add($transformer1);
        $segments->add($processor1);
        $segments->add($transformer2);
        $segments->add($processor2);
        $segments->add($loader);

        $allSegments = $segments->all();

        static::assertCount(3, $allSegments);
        static::assertSame([$transformer1], $allSegments[0]->steps());
        static::assertSame($processor1, $allSegments[0]->processor());
        static::assertSame([$transformer2], $allSegments[1]->steps());
        static::assertSame($processor2, $allSegments[1]->processor());
        static::assertSame([$loader], $allSegments[2]->steps());
        static::assertNull($allSegments[2]->processor());
    }

    public function test_new_segments_has_one_empty_segment(): void
    {
        $segments = new Segments();

        static::assertCount(1, $segments->all());
        static::assertSame([], $segments->all()[0]->steps());
    }

    public function test_extractor_returns_the_extractor_of_the_first_segment(): void
    {
        $extractor = from_rows(rows(schema()));
        $segments = new Segments($extractor);

        $segments->add(new BatchingProcessor(10));
        $segments->add(new SpyTransformer());

        static::assertSame($extractor, $segments->extractor());
    }

    public function test_extractor_is_null_when_the_segments_were_built_without_one(): void
    {
        static::assertNull((new Segments())->extractor());
    }

    public function test_steps_are_flattened_with_each_processor_after_its_segment(): void
    {
        $segments = new Segments();
        $transformer1 = new SpyTransformer();
        $loader1 = new SpyLoader();
        $processor = new PassThroughProcessor();
        $transformer2 = new SpyTransformer();
        $loader2 = new SpyLoader();

        $segments->add($transformer1);
        $segments->add($loader1);
        $segments->add($processor);
        $segments->add($transformer2);
        $segments->add($loader2);

        static::assertSame(
            [$transformer1, $loader1, $processor, $transformer2, $loader2],
            PipelineSteps::of($segments),
        );
    }

    public function test_a_new_segments_has_no_steps(): void
    {
        $segments = new Segments();

        static::assertSame([], PipelineSteps::of($segments));
    }

    public function test_without_a_processor_every_step_stays_in_one_segment(): void
    {
        $segments = new Segments();
        $transformer = new SpyTransformer();
        $loader = new SpyLoader();

        $segments->add($transformer);
        $segments->add($loader);

        static::assertSame([$transformer, $loader], PipelineSteps::of($segments));
    }
}
