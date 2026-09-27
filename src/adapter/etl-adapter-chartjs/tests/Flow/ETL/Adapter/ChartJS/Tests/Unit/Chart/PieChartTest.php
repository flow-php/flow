<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Tests\Unit\Chart;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\ChartJS\pie_chart;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PieChartTest extends FlowTestCase
{
    public function test_datasets_are_read_row_by_row_and_the_last_row_names_the_pie(): void
    {
        $chart = pie_chart(ref('l'), refs(ref('a'), ref('b')));

        $chart->collect(array_to_rows(
            [['l' => 'x', 'a' => 1, 'b' => 10], ['l' => 'y', 'a' => 2, 'b' => 20]],
            schema(str_schema('l'), int_schema('a'), int_schema('b')),
        ));

        static::assertSame(
            [
                'type' => 'pie',
                'data' => ['labels' => ['a', 'b'], 'datasets' => [['data' => [1, 10, 2, 20], 'label' => 'y']]],
            ],
            $chart->data(),
        );
    }

    public function test_an_empty_batch_adds_no_dataset(): void
    {
        $chart = pie_chart(ref('l'), refs(ref('a')));

        $chart->collect(array_to_rows([], schema(str_schema('l'), int_schema('a'))));

        static::assertSame(['type' => 'pie', 'data' => ['labels' => ['a'], 'datasets' => []]], $chart->data());
    }
}
