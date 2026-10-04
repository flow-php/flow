<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Window;

use ArrayObject;
use DateTimeImmutable;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\SpyColumn;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Window\PeerFrame;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function intdiv;
use function range;

final class PeerFrameTest extends FlowTestCase
{
    public function test_all_rows_tied_covers_the_whole_partition(): void
    {
        $partition = array_to_rows([['d' => 1], ['d' => 1], ['d' => 1]], schema(int_schema('d')));

        $frame = new PeerFrame([ref('d')]);

        static::assertSame([0, 2], $frame->bounds(0, $partition));
        static::assertSame([0, 2], $frame->bounds(1, $partition));
        static::assertSame([0, 2], $frame->bounds(2, $partition));
    }

    public function test_the_partition_is_read_once_for_every_row_of_it(): void
    {
        $calls = new ArrayObject();
        $schema = schema(int_schema('v'));
        $column = array_to_rows(
            array_map(static fn(int $i): array => ['v' => intdiv($i, 10)], range(0, 49)),
            $schema,
        )->column('v');
        $partition = Rows::fromColumns($schema, ['v' => new SpyColumn($column, $calls)], 50);
        $frame = new PeerFrame([ref('v')]);
        $ends = [];

        for ($index = 0; $index < 50; $index++) {
            $ends[] = $frame->bounds($index, $partition)[1];
        }

        static::assertSame(array_map(static fn(int $i): int => (intdiv($i, 10) * 10) + 9, range(0, 49)), $ends);
        static::assertCount(1, $calls);
    }

    public function test_datetime_peers_are_matched_by_value_not_identity(): void
    {
        $partition = array_to_rows([
            ['d' => new DateTimeImmutable('2024-01-01 00:00:00')],
            ['d' => new DateTimeImmutable('2024-01-01 00:00:00')],
            ['d' => new DateTimeImmutable('2024-01-02 00:00:00')],
        ], schema(datetime_schema('d')));

        $frame = new PeerFrame([ref('d')]);

        static::assertSame([0, 1], $frame->bounds(0, $partition));
        static::assertSame([0, 1], $frame->bounds(1, $partition));
        static::assertSame([0, 2], $frame->bounds(2, $partition));
    }

    public function test_index_past_the_partition_end_is_empty(): void
    {
        static::assertSame([1, 0], (new PeerFrame([ref('d')]))->bounds(0, rows(schema())));
    }

    public function test_multiple_order_by_references_must_all_match(): void
    {
        $partition = array_to_rows(
            [['a' => 'x', 'b' => 1], ['a' => 'x', 'b' => 1], ['a' => 'x', 'b' => 2]],
            schema(str_schema('a'), int_schema('b')),
        );

        $frame = new PeerFrame([ref('a'), ref('b')]);

        static::assertSame([0, 1], $frame->bounds(0, $partition));
        static::assertSame([0, 2], $frame->bounds(2, $partition));
    }

    public function test_ties_extend_the_frame_to_the_last_peer(): void
    {
        $partition = array_to_rows([['d' => 1], ['d' => 1], ['d' => 2], ['d' => 3]], schema(int_schema('d')));

        $frame = new PeerFrame([ref('d')]);

        static::assertSame([0, 1], $frame->bounds(0, $partition));
        static::assertSame([0, 1], $frame->bounds(1, $partition));
        static::assertSame([0, 2], $frame->bounds(2, $partition));
        static::assertSame([0, 3], $frame->bounds(3, $partition));
    }

    public function test_without_ties_the_frame_ends_at_the_current_row(): void
    {
        $partition = array_to_rows([['d' => 1], ['d' => 2], ['d' => 3]], schema(int_schema('d')));

        $frame = new PeerFrame([ref('d')]);

        static::assertSame([0, 0], $frame->bounds(0, $partition));
        static::assertSame([0, 1], $frame->bounds(1, $partition));
        static::assertSame([0, 2], $frame->bounds(2, $partition));
    }
}
