<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\PeerFrame;
use Flow\ETL\Window\RowsFrame;
use Flow\ETL\Window\WholePartitionFrame;

use function array_map;
use function Flow\ETL\DSL\current_row;
use function Flow\ETL\DSL\preceding;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\window;

final class WindowTest extends FlowTestCase
{
    public function test_explicit_frame_wins_over_the_default(): void
    {
        static::assertInstanceOf(
            RowsFrame::class,
            window()->orderBy(ref('date'))->rowsBetween(preceding(2), current_row())->frame(),
        );
    }

    public function test_explicit_frame_is_allowed_on_an_unordered_window(): void
    {
        static::assertInstanceOf(
            RowsFrame::class,
            window()->partitionBy(ref('dept'))->rowsBetween(preceding(2), current_row())->frame(),
        );
    }

    public function test_order_by_does_not_reset_partition_by(): void
    {
        $window = window()->partitionBy(ref('dept'))->orderBy(ref('date'));

        static::assertSame(['dept'], array_map(static fn($ref) => $ref->name(), $window->partitions()));
        static::assertSame(['date'], array_map(static fn($ref) => $ref->name(), $window->order()));
    }

    public function test_order_is_empty_by_default(): void
    {
        static::assertSame([], window()->order());
        static::assertSame([], window()->partitionBy(ref('dept'))->order());
    }

    public function test_ordered_window_defaults_to_a_peer_frame(): void
    {
        static::assertInstanceOf(PeerFrame::class, window()->orderBy(ref('date'))->frame());
    }

    public function test_partition_by_does_not_destroy_order_by(): void
    {
        $window = window()->orderBy(ref('date'))->partitionBy(ref('dept'));

        static::assertSame(['date'], array_map(static fn($ref) => $ref->name(), $window->order()));
        static::assertSame(['dept'], array_map(static fn($ref) => $ref->name(), $window->partitions()));
    }

    public function test_order_by_returns_a_copy(): void
    {
        $window = window()->orderBy(ref('date'));
        $reordered = $window->orderBy(ref('id'));

        static::assertNotSame($window, $reordered);
        static::assertSame(['date'], array_map(static fn($ref) => $ref->name(), $window->order()));
        static::assertSame(['id'], array_map(static fn($ref) => $ref->name(), $reordered->order()));
    }

    public function test_partition_by_returns_a_copy(): void
    {
        $window = window()->partitionBy(ref('dept'));
        $repartitioned = $window->partitionBy(ref('dept'), ref('country'));

        static::assertNotSame($window, $repartitioned);
        static::assertSame(['dept'], array_map(static fn($ref) => $ref->name(), $window->partitions()));
        static::assertSame(
            ['dept', 'country'],
            array_map(static fn($ref) => $ref->name(), $repartitioned->partitions()),
        );
    }

    public function test_rows_between_returns_a_copy(): void
    {
        $window = window()->orderBy(ref('date'));
        $framed = $window->rowsBetween(preceding(2), current_row());

        static::assertNotSame($window, $framed);
        static::assertInstanceOf(PeerFrame::class, $window->frame());
        static::assertInstanceOf(RowsFrame::class, $framed->frame());
    }

    public function test_unordered_window_defaults_to_the_whole_partition(): void
    {
        static::assertInstanceOf(WholePartitionFrame::class, window()->frame());
        static::assertInstanceOf(WholePartitionFrame::class, window()->partitionBy(ref('dept'))->frame());
    }
}
