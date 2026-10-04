<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Tests\Unit\Chart;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\ChartJS\line_chart;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;

final class LineChartTest extends FlowTestCase
{
    public function test_an_int_label_is_rendered_as_a_string(): void
    {
        $chart = line_chart(ref('l'), refs(ref('v')));

        $chart->collect(array_to_rows([['l' => 1, 'v' => 5]], schema(int_schema('l'), int_schema('v'))));

        static::assertSame(
            ['type' => 'line', 'data' => ['labels' => ['1'], 'datasets' => [['label' => 'v', 'data' => [5]]]]],
            $chart->data(),
        );
    }
}
