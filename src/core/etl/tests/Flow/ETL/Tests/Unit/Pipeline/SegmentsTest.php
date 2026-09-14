<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer;
use Generator;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class SegmentsTest extends FlowTestCase
{
    public function test_add_loader_goes_into_the_only_segment(): void
    {
        $segments = new Segments();
        $loader = $this->createStubLoader();

        $segments->add($loader);

        static::assertCount(1, $segments->all());
        static::assertSame([$loader], $segments->all()[0]->steps());
    }

    public function test_add_multiple_transformers_and_loaders_go_into_the_only_segment(): void
    {
        $segments = new Segments();
        $transformer1 = $this->createStubTransformer();
        $transformer2 = $this->createStubTransformer();
        $loader = $this->createStubLoader();

        $segments->add($transformer1);
        $segments->add($transformer2);
        $segments->add($loader);

        static::assertCount(1, $segments->all());
        static::assertSame([$transformer1, $transformer2, $loader], $segments->all()[0]->steps());
    }

    public function test_add_processor_creates_new_segment(): void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();
        $processor = $this->createStubProcessor();

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
        $processor = $this->createStubProcessor();

        $segments->add($processor);

        $allSegments = $segments->all();

        static::assertCount(2, $allSegments);
        static::assertSame([], $allSegments[0]->steps());
        static::assertSame($processor, $allSegments[0]->processor());
    }

    public function test_add_transformer_goes_into_the_only_segment(): void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();

        $segments->add($transformer);

        static::assertCount(1, $segments->all());
        static::assertSame([$transformer], $segments->all()[0]->steps());
    }

    public function test_all_returns_all_segments_including_current(): void
    {
        $segments = new Segments();
        $transformer1 = $this->createStubTransformer();
        $processor = $this->createStubProcessor();
        $transformer2 = $this->createStubTransformer();

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
        $transformer1 = $this->createStubTransformer();
        $processor1 = $this->createStubProcessor();
        $transformer2 = $this->createStubTransformer();
        $processor2 = $this->createStubProcessor();
        $loader = $this->createStubLoader();

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
        $segments->add($this->createStubTransformer());

        static::assertSame($extractor, $segments->extractor());
    }

    public function test_extractor_is_null_when_the_segments_were_built_without_one(): void
    {
        static::assertNull((new Segments())->extractor());
    }

    public function test_steps_returns_all_steps_flattened_including_processors(): void
    {
        $segments = new Segments();
        $transformer1 = $this->createStubTransformer();
        $loader1 = $this->createStubLoader();
        $processor = $this->createStubProcessor();
        $transformer2 = $this->createStubTransformer();
        $loader2 = $this->createStubLoader();

        $segments->add($transformer1);
        $segments->add($loader1);
        $segments->add($processor);
        $segments->add($transformer2);
        $segments->add($loader2);

        static::assertSame([$transformer1, $loader1, $processor, $transformer2, $loader2], $segments->steps());
    }

    public function test_steps_returns_empty_array_for_new_segments(): void
    {
        $segments = new Segments();

        static::assertSame([], $segments->steps());
    }

    public function test_steps_returns_steps_from_current_segment_only_when_no_processors(): void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();
        $loader = $this->createStubLoader();

        $segments->add($transformer);
        $segments->add($loader);

        static::assertSame([$transformer, $loader], $segments->steps());
    }

    private function createStubLoader(): Loader
    {
        return new class implements Loader {
            public function load(Rows $rows, FlowContext $context): void {}
        };
    }

    private function createStubProcessor(): Processor
    {
        return new class implements Processor {
            public function bind(Schema $input): BoundStep
            {
                return new BoundStep($this, $input);
            }

            public function process(Generator $rows, FlowContext $context): Generator
            {
                yield from $rows;
            }
        };
    }

    private function createStubTransformer(): Transformer
    {
        return new class implements Transformer {
            public function bind(Schema $input): BoundStep
            {
                return new BoundStep($this, $input);
            }

            public function transform(Rows $rows, FlowContext $context): Rows
            {
                return $rows;
            }
        };
    }
}
