<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API;

use Flow\ETL\API\FilteredBranch;
use Flow\ETL\Planning\Logical\Format;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\to_transformation;

final class FilteredBranchTest extends FlowTestCase
{
    public function test_the_condition_becomes_a_write_over_a_filter(): void
    {
        $memory = new ArrayMemory();
        $dataFrame = df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(new FilteredBranch(ref('id')->equals(lit(1)), to_memory($memory)));

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque
            ├─ #2 Result  preserving · transparent
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read  source · transparent
            │        Extractor: ArrayExtractor
            └─ #4 Write  preserving · opaque
               │  Loader: MemoryLoader
               └─ #3 Filter  reducing · transparent
                  │  Condition: id = 1
                  └─ #1 Read (shared)
            PLAN, $dataFrame->explain()->toString(format: Format::declarations));

        $dataFrame->run();

        static::assertSame([['id' => 1]], $memory->dump());
    }

    public function test_a_transformed_branch_filters_before_it_transforms(): void
    {
        static::assertSame(
            <<<'PLAN'
                Outputs  preserving · opaque
                ├─ #2 Result  preserving · transparent
                │  │  Rows this plan hands out: to the trigger, or to the node reading it
                │  └─ #1 Read  source · transparent
                │        Extractor: ArrayExtractor
                └─ #5 Write  preserving · opaque
                   │  Loader: MemoryLoader
                   └─ #4 Select  preserving · transparent · redefines all but id
                      │  Keeps only: id
                      └─ #3 Filter  reducing · transparent
                         │  Condition: id = 1
                         └─ #1 Read (shared)
                PLAN,
            df()
                ->read(from_array([['id' => 1]]))
                ->write(
                    new FilteredBranch(
                        ref('id')->equals(lit(1)),
                        to_transformation(select('id'), to_memory(new ArrayMemory())),
                    ),
                )
                ->explain()
                ->toString(format: Format::declarations),
        );
    }
}
