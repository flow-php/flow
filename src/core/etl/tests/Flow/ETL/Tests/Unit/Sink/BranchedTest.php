<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sink;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Format;
use Flow\ETL\Sink\Branched;
use Flow\ETL\Sink\Transformed;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_memory;

final class BranchedTest extends FlowTestCase
{
    public function test_the_condition_becomes_a_write_over_a_filter(): void
    {
        $memory = new ArrayMemory();
        $dataFrame = df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(new Branched(ref('id')->equals(lit(1)), to_memory($memory)));

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque · streaming
            ├─ #2 Result  preserving · transparent · streaming
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read  source · transparent · streaming
            │        Extractor: ArrayExtractor
            └─ #4 Write  preserving · opaque · streaming
               │  Loader: MemoryLoader
               └─ #3 Filter  reducing · transparent · streaming
                  │  Condition: Equals
                  └─ #1 Read (shared)
            PLAN, $dataFrame->explain()->toString(format: Format::declarations));

        $dataFrame->run();

        static::assertSame([['id' => 1]], $memory->dump());
    }

    public function test_with_transformation_nests_a_transformed_sink(): void
    {
        $condition = ref('id')->equals(lit(1));
        $transformation = select('id');
        $loader = to_memory(new ArrayMemory());

        static::assertEquals(
            new Branched($condition, new Transformed($transformation, $loader)),
            (new Branched($condition, $loader))->withTransformation($transformation),
        );
    }

    public function test_with_transformation_filters_before_it_transforms(): void
    {
        $dataFrame = df()
            ->read(from_array([['id' => 1]]))
            ->write((new Branched(ref('id')->equals(lit(1)), to_memory(new ArrayMemory())))->withTransformation(select(
                'id',
            )));

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque · streaming
            ├─ #2 Result  preserving · transparent · streaming
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read  source · transparent · streaming
            │        Extractor: ArrayExtractor
            └─ #5 Write  preserving · opaque · streaming
               │  Loader: MemoryLoader
               └─ #4 Select  preserving · transparent · streaming
                  └─ #3 Filter  reducing · transparent · streaming
                     │  Condition: Equals
                     └─ #1 Read (shared)
            PLAN, $dataFrame->explain()->toString(format: Format::declarations));
    }
}
