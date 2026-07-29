<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Window\RowsFrame;

use function Flow\ETL\DSL\current_row;
use function Flow\ETL\DSL\following;
use function Flow\ETL\DSL\preceding;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\unbounded_following;
use function Flow\ETL\DSL\unbounded_preceding;

final class RowsFrameTest extends FlowTestCase
{
    public function test_centred_frame_clamps_at_the_first_index(): void
    {
        static::assertSame(
            [0, 1],
            (new RowsFrame(preceding(1), following(1)))->bounds(0, RowsMother::sequentialIds(5)),
        );
    }

    public function test_centred_frame_clamps_at_the_last_index(): void
    {
        static::assertSame(
            [3, 4],
            (new RowsFrame(preceding(1), following(1)))->bounds(4, RowsMother::sequentialIds(5)),
        );
    }

    public function test_empty_partition_yields_an_empty_frame(): void
    {
        static::assertSame([1, 0], (new RowsFrame(unbounded_preceding(), unbounded_following()))->bounds(0, rows()));
    }

    public function test_frame_entirely_after_the_partition_end_is_empty(): void
    {
        static::assertSame(
            [1, 0],
            (new RowsFrame(following(5), following(10)))->bounds(4, RowsMother::sequentialIds(5)),
        );
    }

    public function test_frame_entirely_before_the_partition_start_is_empty(): void
    {
        static::assertSame(
            [1, 0],
            (new RowsFrame(preceding(10), preceding(5)))->bounds(0, RowsMother::sequentialIds(5)),
        );
    }

    public function test_preceding_to_current_row_clamps_at_the_first_index(): void
    {
        static::assertSame(
            [0, 0],
            (new RowsFrame(preceding(2), current_row()))->bounds(0, RowsMother::sequentialIds(5)),
        );
    }

    public function test_preceding_to_current_row_in_the_middle_of_the_partition(): void
    {
        static::assertSame(
            [1, 3],
            (new RowsFrame(preceding(2), current_row()))->bounds(3, RowsMother::sequentialIds(5)),
        );
    }

    public function test_single_row_partition(): void
    {
        static::assertSame(
            [0, 0],
            (new RowsFrame(preceding(2), following(2)))->bounds(0, RowsMother::sequentialIds(1)),
        );
    }

    public function test_start_after_end_is_empty(): void
    {
        static::assertSame(
            [1, 0],
            (new RowsFrame(following(1), preceding(1)))->bounds(2, RowsMother::sequentialIds(5)),
        );
    }

    public function test_unbounded_to_unbounded_covers_the_whole_partition(): void
    {
        static::assertSame(
            [0, 4],
            (new RowsFrame(unbounded_preceding(), unbounded_following()))->bounds(2, RowsMother::sequentialIds(5)),
        );
    }
}
