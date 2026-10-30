<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Functions\Window;
use Flow\ETL\Tests\Context\AggregatorRoundTrip;
use Flow\ETL\Tests\Double\CountingWindowFrame;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\window;

final class RowNumberTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        [$original, $copy] = AggregatorRoundTrip::windowed(row_number()->over(
            window()->partitionBy(ref('d'))->orderBy(ref('v')->desc()),
        ));

        static::assertSame($original, $copy);
    }

    public function test_row_number_is_the_position_in_the_ordered_partition(): void
    {
        static::assertSame(
            5,
            row_number()
                ->over(window()->partitionBy(ref('value'))->orderBy(ref('id')->desc()))
                ->values(
                    array_to_rows(
                        [
                            ['id' => 5, 'value' => 1],
                            ['id' => 4, 'value' => 1],
                            ['id' => 3, 'value' => 1],
                            ['id' => 2, 'value' => 1],
                            ['id' => 1, 'value' => 1],
                        ],
                        schema(int_schema('id'), int_schema('value')),
                    ),
                    flow_context()->backend(),
                )[4],
        );
    }

    public function test_rank_partition_numbers_every_row_in_partition_order(): void
    {
        $rows = array_to_rows([['id' => 3], ['id' => 3], ['id' => 1]], schema(int_schema('id')));
        $rowNumber = row_number()->over(window()->orderBy(ref('id')->desc()));

        static::assertSame([1, 2, 3], $rowNumber->values($rows, flow_context()->backend()));
        static::assertSame([], $rowNumber->values($rows->slice(0, 0), flow_context()->backend()));
    }

    public function test_row_number_numbers_duplicated_rows_separately(): void
    {
        $rows = array_to_rows(
            [['id' => 1, 'value' => 100], ['id' => 1, 'value' => 100]],
            schema(int_schema('id'), int_schema('value')),
        );

        $rowNumber = row_number()->over(window()->orderBy(ref('value')));

        static::assertSame(1, $rowNumber->values($rows, flow_context()->backend())[0]);
        static::assertSame(2, $rowNumber->values($rows, flow_context()->backend())[1]);
    }

    public function test_ranking_never_resolves_a_frame(): void
    {
        $frame = new CountingWindowFrame();

        static::assertSame(
            [1, 2, 3],
            row_number()
                ->over(new Window([], [ref('id')->asc()], $frame))
                ->values(RowsMother::sequentialIds(3), flow_context()->backend()),
        );
        static::assertSame(0, $frame->calls);
    }

    public function test_with_children_returns_the_same_leaf(): void
    {
        $function = row_number();

        static::assertSame([], $function->children());
        static::assertSame($function, $function->withChildren([]));
    }
}
