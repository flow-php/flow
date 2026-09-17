<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sink;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Format;
use Flow\ETL\Sink\Branched;
use Flow\ETL\Sink\Transformed;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_memory;

final class TransformedTest extends FlowTestCase
{
    public function test_a_transformer_becomes_a_write_over_a_transform(): void
    {
        $memory = new ArrayMemory();
        $dataFrame = df()
            ->read(from_array([['id' => 1]]))
            ->write(new Transformed(new AddRowIndexTransformer('idx', StartFrom::ZERO), to_memory($memory)));

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque · streaming
            ├─ #2 Result  preserving · transparent · streaming
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read  source · transparent · streaming
            │        Extractor: ArrayExtractor
            └─ #4 Write  preserving · opaque · streaming
               │  Loader: MemoryLoader
               └─ #3 Transform  unknown · opaque · streaming · redefines unknown
                  └─ #1 Read (shared)
            PLAN, $dataFrame->explain()->toString(format: Format::declarations));

        $dataFrame->run();

        static::assertSame([['id' => 1, 'idx' => 0]], $memory->dump());
    }

    public function test_a_transformation_builds_through_the_frames_verbs(): void
    {
        $dataFrame = df()
            ->read(from_array([['id' => 1]]))
            ->write(new Transformed(select('id'), to_memory(new ArrayMemory())));

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque · streaming
            ├─ #2 Result  preserving · transparent · streaming
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read  source · transparent · streaming
            │        Extractor: ArrayExtractor
            └─ #4 Write  preserving · opaque · streaming
               │  Loader: MemoryLoader
               └─ #3 Select  preserving · transparent · streaming
                  └─ #1 Read (shared)
            PLAN, $dataFrame->explain()->toString(format: Format::declarations));
    }

    public function test_a_sink_child_extends_the_chain(): void
    {
        $dataFrame = df()
            ->read(from_array([['id' => 1]]))
            ->write(
                new Transformed(
                    new AddRowIndexTransformer('idx', StartFrom::ZERO),
                    new Branched(ref('id')->equals(lit(1)), to_memory(new ArrayMemory())),
                ),
            );

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque · streaming
            ├─ #2 Result  preserving · transparent · streaming
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read  source · transparent · streaming
            │        Extractor: ArrayExtractor
            └─ #5 Write  preserving · opaque · streaming
               │  Loader: MemoryLoader
               └─ #4 Filter  reducing · transparent · streaming
                  │  Condition: Equals
                  └─ #3 Transform  unknown · opaque · streaming · redefines unknown
                     └─ #1 Read (shared)
            PLAN, $dataFrame->explain()->toString(format: Format::declarations));
    }

    public function test_a_transformation_returning_another_frame_is_refused(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'A Transformation inside a sink must return the frame it was given; '
            . CallbackTransformation::class
            . ' returned another frame, so its writes would never run',
        );

        df()->read(from_array([['id' => 1]]))->write(new Transformed(
            new CallbackTransformation(static fn(DataFrame $prefix): DataFrame => df()->read(from_array([['x' => 1]]))),
            to_memory(new ArrayMemory()),
        ));
    }
}
