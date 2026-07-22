<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy;
use Flow\ETL\Processor\PivotProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;

final class PivotProcessorTest extends FlowTestCase
{
    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new PivotProcessor(new GroupBy(ref('date')), 0);
    }

    public function test_pivots_grouped_rows(): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'));
        $groupBy->aggregate(sum(ref('contributions')));

        $input = (static function (): Generator {
            yield rows(
                row(str_entry('date', '2024-01-01'), str_entry('user', 'norbert'), int_entry('contributions', 2)),
                row(str_entry('date', '2024-01-01'), str_entry('user', 'stloyd'), int_entry('contributions', 3)),
                row(str_entry('date', '2024-01-02'), str_entry('user', 'norbert'), int_entry('contributions', 5)),
            );
        })();

        $result = iterator_to_array(
            (new PivotProcessor($groupBy))->process($input, flow_context()),
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
                '2024-01-01' => ['date' => '2024-01-01', 'norbert' => 2, 'stloyd' => 3],
                '2024-01-02' => ['date' => '2024-01-02', 'norbert' => 5, 'stloyd' => null],
            ],
            $pivoted,
        );
    }
}
