<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions\Window;

use ArrayObject;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Functions\FrameAccumulating;
use Flow\ETL\Functions\Window\FrameAccumulation;
use Flow\ETL\Functions\Window\PeerFrame;
use Flow\ETL\Functions\Window\RowsFrame;
use Flow\ETL\Functions\Window\WholePartitionFrame;
use Flow\ETL\Functions\Window\WindowFrame;
use Flow\ETL\Tests\Double\CountingFrameAccumulating;
use Flow\ETL\Tests\Double\CountingWindowFrame;
use Flow\ETL\Tests\Double\ProgressSpy;
use Flow\ETL\Tests\Double\SpyColumn;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_fill;
use function array_map;
use function array_slice;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\current_row;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\following;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\preceding;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\unbounded_following;
use function Flow\ETL\DSL\unbounded_preceding;
use function Flow\ETL\DSL\window;
use function range;

final class FrameAccumulationTest extends FlowTestCase
{
    public static function frames_over_floats_with_nulls_and_ties(): Generator
    {
        $sum = sum(ref('v'));
        $average = average(ref('v'));
        $count = count(ref('v'));
        $rows = count();
        $big = 1.0E16;

        yield 'sum, 2 preceding to current' => [
            $sum,
            new RowsFrame(preceding(2), current_row()),
            [0.1, 0.1, 0.30000000000000004, $big, $big, $big, -$big],
        ];
        yield 'average, 2 preceding to current' => [
            $average,
            new RowsFrame(preceding(2), current_row()),
            [0.1, 0.1, 0.15, 5.0E15, 3333333333333333.5, 5.0E15, -5.0E15],
        ];
        yield 'count of v, 2 preceding to current' => [
            $count,
            new RowsFrame(preceding(2), current_row()),
            [1, 1, 2, 2, 3, 2, 2],
        ];
        yield 'count of rows, 2 preceding to current' => [
            $rows,
            new RowsFrame(preceding(2), current_row()),
            [1, 2, 3, 3, 3, 3, 3],
        ];
        yield 'sum, current to 2 following' => [
            $sum,
            new RowsFrame(current_row(), following(2)),
            [0.30000000000000004, $big, $big, $big, -$big, -$big, -$big],
        ];
        yield 'average, current to 2 following' => [
            $average,
            new RowsFrame(current_row(), following(2)),
            [0.15, 5.0E15, 3333333333333333.5, 5.0E15, -5.0E15, -$big, -$big],
        ];
        yield 'count of v, current to 2 following' => [
            $count,
            new RowsFrame(current_row(), following(2)),
            [2, 2, 3, 2, 2, 1, 1],
        ];
        yield 'count of rows, current to 2 following' => [
            $rows,
            new RowsFrame(current_row(), following(2)),
            [3, 3, 3, 3, 3, 2, 1],
        ];
        yield 'sum, 1 preceding to 1 following' => [
            $sum,
            new RowsFrame(preceding(1), following(1)),
            [0.1, 0.30000000000000004, $big, $big, $big, -$big, -$big],
        ];
        yield 'average, 1 preceding to 1 following' => [
            $average,
            new RowsFrame(preceding(1), following(1)),
            [0.1, 0.15, 5.0E15, 3333333333333333.5, 5.0E15, -5.0E15, -$big],
        ];
        yield 'count of v, 1 preceding to 1 following' => [
            $count,
            new RowsFrame(preceding(1), following(1)),
            [1, 2, 2, 3, 2, 2, 1],
        ];
        yield 'sum, 3 preceding to 1 preceding' => [
            $sum,
            new RowsFrame(preceding(3), preceding(1)),
            [null, 0.1, 0.1, 0.30000000000000004, $big, $big, $big],
        ];
        yield 'average, 3 preceding to 1 preceding' => [
            $average,
            new RowsFrame(preceding(3), preceding(1)),
            [null, 0.1, 0.1, 0.15, 5.0E15, 3333333333333333.5, 5.0E15],
        ];
        yield 'count of rows, 3 preceding to 1 preceding' => [
            $rows,
            new RowsFrame(preceding(3), preceding(1)),
            [0, 1, 2, 3, 3, 3, 3],
        ];
        yield 'sum, 1 following to 3 following' => [
            $sum,
            new RowsFrame(following(1), following(3)),
            [$big, $big, $big, -$big, -$big, -$big, null],
        ];
        yield 'count of v, 1 following to 3 following' => [
            $count,
            new RowsFrame(following(1), following(3)),
            [2, 3, 2, 2, 1, 1, 0],
        ];
        yield 'sum, unbounded preceding to current' => [
            $sum,
            new RowsFrame(unbounded_preceding(), current_row()),
            [0.1, 0.1, 0.30000000000000004, $big, $big, $big, 0.6000000000000001],
        ];
        yield 'average, unbounded preceding to current' => [
            $average,
            new RowsFrame(unbounded_preceding(), current_row()),
            [0.1, 0.1, 0.15, 3333333333333333.5, 2.5E15, 2.5E15, 0.12],
        ];
        yield 'sum, current to unbounded following' => [
            $sum,
            new RowsFrame(current_row(), unbounded_following()),
            [0.6000000000000001, 0.5, 0.5, 0.3, -$big, -$big, -$big],
        ];
        yield 'average, current to unbounded following' => [
            $average,
            new RowsFrame(current_row(), unbounded_following()),
            [0.12, 0.13, 0.13, 0.1, -5.0E15, -$big, -$big],
        ];
        yield 'count of v, current to unbounded following' => [
            $count,
            new RowsFrame(current_row(), unbounded_following()),
            [5, 4, 4, 3, 2, 1, 1],
        ];
        yield 'sum, unbounded on both sides' => [
            $sum,
            new RowsFrame(unbounded_preceding(), unbounded_following()),
            array_fill(0, 7, 0.6000000000000001),
        ];
        yield 'sum, peers of d' => [
            $sum,
            new PeerFrame([ref('d')->asc()]),
            [0.1, 0.1, $big, $big, $big, $big, 0.6000000000000001],
        ];
        yield 'average, peers of d' => [
            $average,
            new PeerFrame([ref('d')->asc()]),
            [0.1, 0.1, 2.5E15, 2.5E15, 2.5E15, 2.5E15, 0.12],
        ];
        yield 'count of rows, peers of d' => [$rows, new PeerFrame([ref('d')->asc()]), [2, 2, 5, 5, 5, 6, 7]];
    }

    public static function frames_over_integers_past_the_int_range(): Generator
    {
        $max = 9.223372036854776E+18;

        yield 'sum, 1 preceding to current' => [
            sum(ref('v')),
            new RowsFrame(preceding(1), current_row()),
            [$max, $max, -$max, -$max, 2.0],
        ];
        yield 'average, 1 preceding to current' => [
            average(ref('v')),
            new RowsFrame(preceding(1), current_row()),
            [$max, 4.6116860184274E+18, -4.611686018427388E+18, -4.611686018427388E+18, 1.0],
        ];
        yield 'sum, 1 preceding to 1 following' => [
            sum(ref('v')),
            new RowsFrame(preceding(1), following(1)),
            [$max, 0.0, -$max, -$max, 2.0],
        ];
        yield 'average, 1 preceding to 1 following' => [
            average(ref('v')),
            new RowsFrame(preceding(1), following(1)),
            [4.6116860184274E+18, 0.0, -3.0744573456182584E+18, -3.0744573456182584E+18, 1.0],
        ];
    }

    /**
     * @param list<null|float|int> $expected
     */
    #[DataProvider('frames_over_floats_with_nulls_and_ties')]
    public function test_a_frame_aggregate_over_floats_with_nulls_and_ties(
        FrameAccumulating $function,
        WindowFrame $frame,
        array $expected,
    ): void {
        static::assertSame($expected, (new FrameAccumulation($frame))->values(
            $function,
            array_to_rows(
                [
                    ['d' => 1, 'v' => 0.1],
                    ['d' => 1, 'v' => null],
                    ['d' => 2, 'v' => 0.2],
                    ['d' => 2, 'v' => 1.0E16],
                    ['d' => 2, 'v' => 0.3],
                    ['d' => 3, 'v' => null],
                    ['d' => 4, 'v' => -1.0E16],
                ],
                schema(int_schema('d'), float_schema('v', nullable: true)),
            ),
            flow_context()->backend(),
            new VoidProgress(),
        ));
    }

    /**
     * @param list<float> $expected
     */
    #[DataProvider('frames_over_integers_past_the_int_range')]
    public function test_a_frame_aggregate_over_integers_past_the_int_range(
        FrameAccumulating $function,
        WindowFrame $frame,
        array $expected,
    ): void {
        static::assertSame($expected, (new FrameAccumulation($frame))->values(
            $function,
            array_to_rows([
                ['v' => PHP_INT_MAX],
                ['v' => 1],
                ['v' => -PHP_INT_MAX],
                ['v' => 5],
                ['v' => -3],
            ], schema(int_schema('v'))),
            flow_context()->backend(),
            new VoidProgress(),
        ));
    }

    public function test_an_empty_partition_has_no_values(): void
    {
        static::assertSame(
            [],
            (new FrameAccumulation(new RowsFrame(preceding(1), following(1))))->values(
                sum(ref('id')),
                RowsMother::empty(),
                flow_context()->backend(),
                new VoidProgress(),
            ),
        );
    }

    public function test_a_one_row_partition_frames_only_that_row(): void
    {
        $one = RowsMother::sequentialIds(1);

        static::assertSame(
            [1.0],
            (new FrameAccumulation(new RowsFrame(preceding(1), following(1))))->values(
                sum(ref('id')),
                $one,
                flow_context()->backend(),
                new VoidProgress(),
            ),
        );
        static::assertSame(
            [0],
            (new FrameAccumulation(new RowsFrame(preceding(1), preceding(1))))->values(
                count(),
                $one,
                flow_context()->backend(),
                new VoidProgress(),
            ),
        );
    }

    public function test_a_whole_partition_frame_hands_every_row_the_same_bounds_in_one_call(): void
    {
        $function = new CountingFrameAccumulating(ref('id'));

        static::assertSame(
            [15.0, 15.0, 15.0, 15.0, 15.0],
            (new FrameAccumulation(new WholePartitionFrame()))->values(
                $function,
                RowsMother::sequentialIds(5),
                flow_context()->backend(),
                new VoidProgress(),
            ),
        );
        static::assertSame([array_fill(0, 5, [0, 4])], $function->bounds);
    }

    public function test_a_rows_frame_hands_every_row_its_own_bounds_in_one_call(): void
    {
        $function = new CountingFrameAccumulating(ref('id'));

        static::assertSame(
            [1.0, 3.0, 5.0, 7.0, 9.0],
            (new FrameAccumulation(new RowsFrame(preceding(1), current_row())))->values(
                $function,
                RowsMother::sequentialIds(5),
                flow_context()->backend(),
                new VoidProgress(),
            ),
        );
        static::assertSame([[[0, 0], [0, 1], [1, 2], [2, 3], [3, 4]]], $function->bounds);
    }

    public function test_a_rows_frame_reads_only_the_rows_between_its_bounds(): void
    {
        static::assertSame(
            [1.0, 3.0, 6.0, 9.0, 12.0],
            (new FrameAccumulation(new RowsFrame(preceding(2), current_row())))->values(
                new CountingFrameAccumulating(ref('id')),
                RowsMother::sequentialIds(5),
                flow_context()->backend(),
                new VoidProgress(),
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
                new VoidProgress(),
            ),
        );
        static::assertSame([[[0, 1], [0, 1], [0, 3], [0, 3], [0, 4]]], $function->bounds);
    }

    public function test_an_empty_frame_gives_the_value_of_nothing_accumulated(): void
    {
        static::assertSame(
            [null, null, null],
            (new FrameAccumulation(new RowsFrame(preceding(10), preceding(5))))->values(
                sum(ref('id'))->over(window()),
                RowsMother::sequentialIds(3),
                flow_context()->backend(),
                new VoidProgress(),
            ),
        );
    }

    public static function frame_aggregates(): Generator
    {
        yield 'sum' => [sum(ref('v')), [1.0, 3.0, 6.0, 9.0, 12.0]];
        yield 'average' => [average(ref('v')), [1.0, 1.5, 2.0, 3.0, 4.0]];
        yield 'count' => [count(ref('v')), [1, 2, 3, 3, 3]];
    }

    /**
     * @param list<float|int> $expected
     */
    #[DataProvider('frame_aggregates')]
    public function test_a_frame_aggregate_reads_its_column_once_per_partition(
        FrameAccumulating $function,
        array $expected,
    ): void {
        $calls = new ArrayObject();
        $schema = schema(int_schema('v'));

        static::assertSame($expected, (new FrameAccumulation(new RowsFrame(preceding(2), current_row())))->values(
            $function,
            Rows::fromColumns(
                $schema,
                ['v' => new SpyColumn(
                    array_to_rows(array_map(static fn(int $v): array => ['v' => $v], range(1, 5)), $schema)->column(
                        'v',
                    ),
                    $calls,
                )],
                5,
            ),
            flow_context()->backend(),
            new VoidProgress(),
        ));
        static::assertSame(['values'], $calls->getArrayCopy());
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
                new VoidProgress(),
            ),
        );
        static::assertSame(5, $frame->calls);
    }

    public function test_reports_the_bounds_of_a_long_partition_in_chunks(): void
    {
        $progress = new ProgressSpy();
        $function = new CountingFrameAccumulating(ref('id'));

        (new FrameAccumulation(new RowsFrame(preceding(1), current_row())))->values(
            $function,
            RowsMother::sequentialIds(2500),
            flow_context()->backend(),
            $progress->stage(StageName::window),
        );

        static::assertSame(
            [['window', 1000], ['window', 1000], ['window', 500]],
            array_slice($progress->advancedCalls(), 0, 3),
        );
        static::assertCount(2500, $function->bounds[0]);
    }
}
