<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer;
use Generator;

final class SegmentsTest extends FlowTestCase
{
    public function test_add_loader_to_current_segment(): void
    {
        $segments = new Segments();
        $loader = $this->createStubLoader();

        $segments->add($loader);

        static::assertCount(1, $segments->all());
        static::assertSame([$loader], $segments->current()->steps());
    }

    public function test_add_multiple_transformers_and_loaders_to_current_segment(): void
    {
        $segments = new Segments();
        $transformer1 = $this->createStubTransformer();
        $transformer2 = $this->createStubTransformer();
        $loader = $this->createStubLoader();

        $segments->add($transformer1);
        $segments->add($transformer2);
        $segments->add($loader);

        static::assertCount(1, $segments->all());
        static::assertSame([$transformer1, $transformer2, $loader], $segments->current()->steps());
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

    public function test_add_transformer_to_current_segment(): void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();

        $segments->add($transformer);

        static::assertCount(1, $segments->all());
        static::assertSame([$transformer], $segments->current()->steps());
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

    public function test_current_returns_current_segment_when_no_processors(): void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();

        $segments->add($transformer);

        static::assertSame([$transformer], $segments->current()->steps());
        static::assertNull($segments->current()->processor());
    }

    public function test_current_returns_last_completed_segment_when_processors_exist(): void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();
        $processor = $this->createStubProcessor();

        $segments->add($transformer);
        $segments->add($processor);

        static::assertSame([$transformer], $segments->current()->steps());
        static::assertSame($processor, $segments->current()->processor());
    }

    public function test_has_finds_loader_in_completed_segment(): void
    {
        $segments = new Segments();
        $loader = $this->createStubLoader();
        $processor = $this->createStubProcessor();

        $segments->add($loader);
        $segments->add($processor);

        static::assertTrue($segments->has($loader::class));
    }

    public function test_has_finds_loader_in_current_segment(): void
    {
        $segments = new Segments();
        $loader = $this->createStubLoader();

        $segments->add($loader);

        static::assertTrue($segments->has($loader::class));
    }

    public function test_has_finds_processor_in_completed_segment(): void
    {
        $segments = new Segments();
        $processor = $this->createStubProcessor();

        $segments->add($processor);

        static::assertTrue($segments->has($processor::class));
    }

    public function test_has_finds_transformer_in_completed_segment(): void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();
        $processor = $this->createStubProcessor();

        $segments->add($transformer);
        $segments->add($processor);

        static::assertTrue($segments->has($transformer::class));
    }

    public function test_has_finds_transformer_in_current_segment(): void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();

        $segments->add($transformer);

        static::assertTrue($segments->has($transformer::class));
    }

    public function test_has_returns_false_when_class_not_present(): void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();

        $segments->add($transformer);

        static::assertFalse($segments->has(Loader::class));
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
        static::assertSame([], $segments->current()->steps());
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
