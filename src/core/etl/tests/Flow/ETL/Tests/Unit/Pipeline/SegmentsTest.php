<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\{FlowContext, Loader, Processor, Rows, Transformer};
use Flow\ETL\Pipeline\Segments;
use Flow\ETL\Tests\FlowTestCase;

final class SegmentsTest extends FlowTestCase
{
    public function test_add_loader_to_current_segment() : void
    {
        $segments = new Segments();
        $loader = $this->createStubLoader();

        $segments->add($loader);

        self::assertCount(1, $segments->all());
        self::assertSame([$loader], $segments->current()->steps());
    }

    public function test_add_multiple_transformers_and_loaders_to_current_segment() : void
    {
        $segments = new Segments();
        $transformer1 = $this->createStubTransformer();
        $transformer2 = $this->createStubTransformer();
        $loader = $this->createStubLoader();

        $segments->add($transformer1);
        $segments->add($transformer2);
        $segments->add($loader);

        self::assertCount(1, $segments->all());
        self::assertSame([$transformer1, $transformer2, $loader], $segments->current()->steps());
    }

    public function test_add_processor_creates_new_segment() : void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();
        $processor = $this->createStubProcessor();

        $segments->add($transformer);
        $segments->add($processor);

        $allSegments = $segments->all();

        self::assertCount(2, $allSegments);
        self::assertSame([$transformer], $allSegments[0]->steps());
        self::assertSame($processor, $allSegments[0]->processor());
        self::assertSame([], $allSegments[1]->steps());
        self::assertNull($allSegments[1]->processor());
    }

    public function test_add_processor_with_empty_steps_creates_segment_with_processor() : void
    {
        $segments = new Segments();
        $processor = $this->createStubProcessor();

        $segments->add($processor);

        $allSegments = $segments->all();

        self::assertCount(2, $allSegments);
        self::assertSame([], $allSegments[0]->steps());
        self::assertSame($processor, $allSegments[0]->processor());
    }

    public function test_add_transformer_to_current_segment() : void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();

        $segments->add($transformer);

        self::assertCount(1, $segments->all());
        self::assertSame([$transformer], $segments->current()->steps());
    }

    public function test_all_returns_all_segments_including_current() : void
    {
        $segments = new Segments();
        $transformer1 = $this->createStubTransformer();
        $processor = $this->createStubProcessor();
        $transformer2 = $this->createStubTransformer();

        $segments->add($transformer1);
        $segments->add($processor);
        $segments->add($transformer2);

        $allSegments = $segments->all();

        self::assertCount(2, $allSegments);
        self::assertSame([$transformer1], $allSegments[0]->steps());
        self::assertSame($processor, $allSegments[0]->processor());
        self::assertSame([$transformer2], $allSegments[1]->steps());
        self::assertNull($allSegments[1]->processor());
    }

    public function test_current_returns_current_segment_when_no_processors() : void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();

        $segments->add($transformer);

        self::assertSame([$transformer], $segments->current()->steps());
        self::assertNull($segments->current()->processor());
    }

    public function test_current_returns_last_completed_segment_when_processors_exist() : void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();
        $processor = $this->createStubProcessor();

        $segments->add($transformer);
        $segments->add($processor);

        self::assertSame([$transformer], $segments->current()->steps());
        self::assertSame($processor, $segments->current()->processor());
    }

    public function test_has_finds_loader_in_completed_segment() : void
    {
        $segments = new Segments();
        $loader = $this->createStubLoader();
        $processor = $this->createStubProcessor();

        $segments->add($loader);
        $segments->add($processor);

        self::assertTrue($segments->has($loader::class));
    }

    public function test_has_finds_loader_in_current_segment() : void
    {
        $segments = new Segments();
        $loader = $this->createStubLoader();

        $segments->add($loader);

        self::assertTrue($segments->has($loader::class));
    }

    public function test_has_finds_processor_in_completed_segment() : void
    {
        $segments = new Segments();
        $processor = $this->createStubProcessor();

        $segments->add($processor);

        self::assertTrue($segments->has($processor::class));
    }

    public function test_has_finds_transformer_in_completed_segment() : void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();
        $processor = $this->createStubProcessor();

        $segments->add($transformer);
        $segments->add($processor);

        self::assertTrue($segments->has($transformer::class));
    }

    public function test_has_finds_transformer_in_current_segment() : void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();

        $segments->add($transformer);

        self::assertTrue($segments->has($transformer::class));
    }

    public function test_has_returns_false_when_class_not_present() : void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();

        $segments->add($transformer);

        self::assertFalse($segments->has(Loader::class));
    }

    public function test_multiple_processors_create_multiple_segments() : void
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

        self::assertCount(3, $allSegments);
        self::assertSame([$transformer1], $allSegments[0]->steps());
        self::assertSame($processor1, $allSegments[0]->processor());
        self::assertSame([$transformer2], $allSegments[1]->steps());
        self::assertSame($processor2, $allSegments[1]->processor());
        self::assertSame([$loader], $allSegments[2]->steps());
        self::assertNull($allSegments[2]->processor());
    }

    public function test_new_segments_has_one_empty_segment() : void
    {
        $segments = new Segments();

        self::assertCount(1, $segments->all());
        self::assertSame([], $segments->current()->steps());
    }

    public function test_steps_returns_all_steps_flattened_including_processors() : void
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

        self::assertSame(
            [$transformer1, $loader1, $processor, $transformer2, $loader2],
            $segments->steps()
        );
    }

    public function test_steps_returns_empty_array_for_new_segments() : void
    {
        $segments = new Segments();

        self::assertSame([], $segments->steps());
    }

    public function test_steps_returns_steps_from_current_segment_only_when_no_processors() : void
    {
        $segments = new Segments();
        $transformer = $this->createStubTransformer();
        $loader = $this->createStubLoader();

        $segments->add($transformer);
        $segments->add($loader);

        self::assertSame([$transformer, $loader], $segments->steps());
    }

    private function createStubLoader() : Loader
    {
        return new class implements Loader {
            public function load(Rows $rows, FlowContext $context) : void
            {
            }
        };
    }

    private function createStubProcessor() : Processor
    {
        return new class implements Processor {
            public function process(\Generator $rows, FlowContext $context) : \Generator
            {
                yield from $rows;
            }
        };
    }

    private function createStubTransformer() : Transformer
    {
        return new class implements Transformer {
            public function transform(Rows $rows, FlowContext $context) : Rows
            {
                return $rows;
            }
        };
    }
}
