<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API;

use Flow\ETL\API\DataFrame;
use Flow\ETL\API\FilteredBranch;
use Flow\ETL\API\TransformedBranch;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom;
use Flow\ETL\Operators\Transformer\AddRowIndexTransformer;
use Flow\ETL\Planning\Logical\Format;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\to_memory;

final class TransformedBranchTest extends FlowTestCase
{
    public function test_a_transformer_becomes_a_write_over_a_transform(): void
    {
        $memory = new ArrayMemory();
        $dataFrame = df()
            ->read(from_array([['id' => 1]]))
            ->write(new TransformedBranch(new AddRowIndexTransformer('idx', StartFrom::ZERO), to_memory($memory)));

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque
            ├─ #2 Result  preserving · transparent
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read  source · transparent
            │        Extractor: ArrayExtractor
            └─ #4 Write  preserving · opaque
               │  Loader: MemoryLoader
               └─ #3 Transform  unknown · opaque · redefines unknown
                  └─ #1 Read (shared)
            PLAN, $dataFrame->explain()->toString(format: Format::declarations));

        $dataFrame->run();

        static::assertSame([['id' => 1, 'idx' => 0]], $memory->dump());
    }

    public function test_a_transformation_builds_through_the_frames_verbs(): void
    {
        static::assertSame(
            <<<'PLAN'
                Outputs  preserving · opaque
                ├─ #2 Result  preserving · transparent
                │  │  Rows this plan hands out: to the trigger, or to the node reading it
                │  └─ #1 Read  source · transparent
                │        Extractor: ArrayExtractor
                └─ #4 Write  preserving · opaque
                   │  Loader: MemoryLoader
                   └─ #3 Select  preserving · transparent · redefines all but id
                      │  Keeps only: id
                      └─ #1 Read (shared)
                PLAN,
            df()
                ->read(from_array([['id' => 1]]))
                ->write(new TransformedBranch(select('id'), to_memory(new ArrayMemory())))
                ->explain()
                ->toString(format: Format::declarations),
        );
    }

    public function test_a_branch_child_extends_the_chain(): void
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
                   └─ #4 Filter  reducing · transparent
                      │  Condition: id = 1
                      └─ #3 Transform  unknown · opaque · redefines unknown
                         └─ #1 Read (shared)
                PLAN,
            df()
                ->read(from_array([['id' => 1]]))
                ->write(
                    new TransformedBranch(
                        new AddRowIndexTransformer('idx', StartFrom::ZERO),
                        new FilteredBranch(ref('id')->equals(lit(1)), to_memory(new ArrayMemory())),
                    ),
                )
                ->explain()
                ->toString(format: Format::declarations),
        );
    }

    public function test_a_transformation_returning_another_frame_is_refused(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'A Transformation inside a branch must return the frame it was given; '
            . CallbackTransformation::class
            . ' returned another frame, so its writes would never run',
        );

        df()->read(from_array([['id' => 1]]))->write(new TransformedBranch(
            new CallbackTransformation(static fn(DataFrame $prefix): DataFrame => df()->read(from_array([['x' => 1]]))),
            to_memory(new ArrayMemory()),
        ));
    }
}
