<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions\Window;

use Flow\ETL\Functions\Window\FrameAccumulation;
use Flow\ETL\Functions\Window\PeerFrame;
use Flow\ETL\Functions\Window\RowsFrame;
use Flow\ETL\Functions\Window\WholePartitionFrame;
use Flow\ETL\Tests\Double\CountingFrameAccumulating;
use Flow\ETL\Tests\Double\CountingWindowFrame;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\current_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\preceding;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;

final class FrameAccumulationTest extends FlowTestCase
{
    public function test_a_whole_partition_frame_is_accumulated_once_and_reused_for_every_row(): void
    {
        $function = new CountingFrameAccumulating(ref('id'));

        static::assertSame(
            [15.0, 15.0, 15.0, 15.0, 15.0],
            (new FrameAccumulation(new WholePartitionFrame()))->values(
                $function,
                RowsMother::sequentialIds(5),
                flow_context()->backend(),
            ),
        );
        static::assertSame(1, $function->accumulatorCalls);
        static::assertSame(5, $function->fedRows);
    }

    public function test_a_rows_frame_feeds_only_the_new_tail_while_its_start_stays(): void
    {
        $function = new CountingFrameAccumulating(ref('id'));

        static::assertSame(
            [1.0, 3.0, 5.0, 7.0, 9.0],
            (new FrameAccumulation(new RowsFrame(preceding(1), current_row())))->values(
                $function,
                RowsMother::sequentialIds(5),
                flow_context()->backend(),
            ),
        );
        static::assertSame(4, $function->accumulatorCalls);
        static::assertSame(8, $function->fedRows);
    }

    public function test_a_rows_frame_reads_only_the_rows_between_its_bounds(): void
    {
        static::assertSame(
            [1.0, 3.0, 6.0, 9.0, 12.0],
            (new FrameAccumulation(new RowsFrame(preceding(2), current_row())))->values(
                new CountingFrameAccumulating(ref('id')),
                RowsMother::sequentialIds(5),
                flow_context()->backend(),
            ),
        );
    }

    public function test_a_peer_frame_grows_through_ties(): void
    {
        $function = new CountingFrameAccumulating(ref('id'));

        static::assertSame(
            [3.0, 3.0, 10.0, 10.0, 15.0],
            (new FrameAccumulation(new PeerFrame([ref('d')->asc()])))->values(
                $function,
                array_to_rows(
                    [
                        ['id' => 1, 'd' => 1],
                        ['id' => 2, 'd' => 1],
                        ['id' => 3, 'd' => 2],
                        ['id' => 4, 'd' => 2],
                        ['id' => 5, 'd' => 3],
                    ],
                    schema(int_schema('id'), int_schema('d')),
                ),
                flow_context()->backend(),
            ),
        );
        static::assertSame(1, $function->accumulatorCalls);
        static::assertSame(5, $function->fedRows);
    }

    public function test_an_empty_frame_gives_the_value_of_nothing_accumulated(): void
    {
        static::assertSame(
            [null, null, null],
            (new FrameAccumulation(new RowsFrame(preceding(10), preceding(5))))->values(
                sum(ref('id'))->over(window()),
                RowsMother::sequentialIds(3),
                flow_context()->backend(),
            ),
        );
    }

    public function test_bounds_are_resolved_once_per_row(): void
    {
        $frame = new CountingWindowFrame([0, 2]);

        static::assertSame(
            [6.0, 6.0, 6.0, 6.0, 6.0],
            (new FrameAccumulation($frame))->values(
                new CountingFrameAccumulating(ref('id')),
                RowsMother::sequentialIds(5),
                flow_context()->backend(),
            ),
        );
        static::assertSame(5, $frame->calls);
    }
}
