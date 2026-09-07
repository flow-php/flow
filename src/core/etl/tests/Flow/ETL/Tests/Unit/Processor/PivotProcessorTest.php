<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\Pivot;
use Flow\ETL\Processor\PivotProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class PivotProcessorTest extends FlowTestCase
{
    public function test_bind_derives_the_group_by_columns_and_one_nullable_column_per_pivot_value(): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'), $values = pivot_values('norbert', 'stloyd'));
        $groupBy->aggregate(sum(ref('contributions')));

        static::assertEquals(
            schema(str_schema('date'), float_schema('norbert', nullable: true), float_schema('stloyd', nullable: true)),
            (new PivotProcessor($groupBy, new Pivot(ref('user'), $values)))->bind(schema(
                str_schema('date'),
                str_schema('user'),
                int_schema('contributions'),
            ))->output,
        );
    }

    public function test_bind_refuses_a_pivot_value_colliding_with_a_group_by_column(): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'), $values = pivot_values('date'));
        $groupBy->aggregate(sum(ref('contributions')));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pivot value "date" collides with the group-by column of the same name');

        (new PivotProcessor($groupBy, new Pivot(ref('user'), $values)))->bind(schema(
            str_schema('date'),
            str_schema('user'),
            int_schema('contributions'),
        ));
    }

    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new PivotProcessor(new GroupBy(ref('date')), new Pivot(ref('user'), pivot_values('x')), 0);
    }

    public function test_pivots_grouped_rows(): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'), $values = pivot_values('norbert', 'stloyd'));
        $groupBy->aggregate(sum(ref('contributions')));

        $input = (static function (): Generator {
            yield rows(
                schema(str_schema('date'), str_schema('user'), int_schema('contributions')),
                row(['date' => '2024-01-01', 'user' => 'norbert', 'contributions' => 2]),
                row(['date' => '2024-01-01', 'user' => 'stloyd', 'contributions' => 3]),
                row(['date' => '2024-01-02', 'user' => 'norbert', 'contributions' => 5]),
            );
        })();

        $result = iterator_to_array(
            (new PivotProcessor($groupBy, new Pivot(ref('user'), $values)))->process($input, flow_context()),
            preserve_keys: false,
        );

        $pivoted = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $pivotRow) {
                $pivoted[$pivotRow['date']] = $pivotRow;
            }
        }

        static::assertSame(
            [
                '2024-01-01' => ['date' => '2024-01-01', 'norbert' => 2.0, 'stloyd' => 3.0],
                '2024-01-02' => ['date' => '2024-01-02', 'norbert' => 5.0, 'stloyd' => null],
            ],
            $pivoted,
        );
    }
}
