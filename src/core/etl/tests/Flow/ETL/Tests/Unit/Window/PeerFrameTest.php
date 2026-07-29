<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\PeerFrame;

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class PeerFrameTest extends FlowTestCase
{
    public function test_all_rows_tied_covers_the_whole_partition(): void
    {
        $partition = rows(row(int_entry('d', 1)), row(int_entry('d', 1)), row(int_entry('d', 1)));

        $frame = new PeerFrame([ref('d')]);

        static::assertSame([0, 2], $frame->bounds(0, $partition));
        static::assertSame([0, 2], $frame->bounds(1, $partition));
        static::assertSame([0, 2], $frame->bounds(2, $partition));
    }

    public function test_datetime_peers_are_matched_by_value_not_identity(): void
    {
        $partition = rows(
            row(datetime_entry('d', new DateTimeImmutable('2024-01-01 00:00:00'))),
            row(datetime_entry('d', new DateTimeImmutable('2024-01-01 00:00:00'))),
            row(datetime_entry('d', new DateTimeImmutable('2024-01-02 00:00:00'))),
        );

        $frame = new PeerFrame([ref('d')]);

        static::assertSame([0, 1], $frame->bounds(0, $partition));
        static::assertSame([0, 1], $frame->bounds(1, $partition));
        static::assertSame([0, 2], $frame->bounds(2, $partition));
    }

    public function test_index_past_the_partition_end_is_empty(): void
    {
        static::assertSame([1, 0], (new PeerFrame([ref('d')]))->bounds(0, rows()));
    }

    public function test_multiple_order_by_references_must_all_match(): void
    {
        $partition = rows(
            row(str_entry('a', 'x'), int_entry('b', 1)),
            row(str_entry('a', 'x'), int_entry('b', 1)),
            row(str_entry('a', 'x'), int_entry('b', 2)),
        );

        $frame = new PeerFrame([ref('a'), ref('b')]);

        static::assertSame([0, 1], $frame->bounds(0, $partition));
        static::assertSame([0, 2], $frame->bounds(2, $partition));
    }

    public function test_ties_extend_the_frame_to_the_last_peer(): void
    {
        $partition = rows(
            row(int_entry('d', 1)),
            row(int_entry('d', 1)),
            row(int_entry('d', 2)),
            row(int_entry('d', 3)),
        );

        $frame = new PeerFrame([ref('d')]);

        static::assertSame([0, 1], $frame->bounds(0, $partition));
        static::assertSame([0, 1], $frame->bounds(1, $partition));
        static::assertSame([0, 2], $frame->bounds(2, $partition));
        static::assertSame([0, 3], $frame->bounds(3, $partition));
    }

    public function test_without_ties_the_frame_ends_at_the_current_row(): void
    {
        $partition = rows(row(int_entry('d', 1)), row(int_entry('d', 2)), row(int_entry('d', 3)));

        $frame = new PeerFrame([ref('d')]);

        static::assertSame([0, 0], $frame->bounds(0, $partition));
        static::assertSame([0, 1], $frame->bounds(1, $partition));
        static::assertSame([0, 2], $frame->bounds(2, $partition));
    }
}
