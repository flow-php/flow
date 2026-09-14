<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Lowering;

use Flow\ETL\Plan\Node\RowIndex;
use Flow\ETL\Planner\Lowering\RowIndexLowering;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;

final class RowIndexLoweringTest extends FlowTestCase
{
    public function test_handles_row_index(): void
    {
        static::assertSame(RowIndex::class, (new RowIndexLowering())->handles());
    }

    public function test_every_lowering_builds_a_fresh_transformer(): void
    {
        $node = new RowIndex(NodeMother::read(), 'idx', StartFrom::ZERO);
        $lowering = new RowIndexLowering();

        $first = $lowering->steps($node, NodeMother::context(), []);
        $second = $lowering->steps($node, NodeMother::context(), []);

        static::assertCount(1, $first);
        static::assertInstanceOf(AddRowIndexTransformer::class, $first[0]);
        static::assertNotSame($first[0], $second[0]);
    }
}
