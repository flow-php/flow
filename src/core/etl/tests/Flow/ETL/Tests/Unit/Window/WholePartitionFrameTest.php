<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Window\WholePartitionFrame;

use function Flow\ETL\DSL\rows;

final class WholePartitionFrameTest extends FlowTestCase
{
    public function test_covers_the_whole_partition_regardless_of_the_index(): void
    {
        $frame = new WholePartitionFrame();

        static::assertSame([0, 4], $frame->bounds(0, RowsMother::sequentialIds(5)));
        static::assertSame([0, 4], $frame->bounds(2, RowsMother::sequentialIds(5)));
        static::assertSame([0, 4], $frame->bounds(4, RowsMother::sequentialIds(5)));
    }

    public function test_empty_partition_yields_an_empty_frame(): void
    {
        static::assertSame([0, -1], (new WholePartitionFrame())->bounds(0, rows()));
    }

    public function test_single_row_partition(): void
    {
        static::assertSame([0, 0], (new WholePartitionFrame())->bounds(0, RowsMother::sequentialIds(1)));
    }
}
