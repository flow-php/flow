<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\CountsByGroup;
use Flow\ETL\Functions\TotalsByGroup;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class TotalsByGroupTest extends FlowTestCase
{
    public function test_a_sum_and_an_average_over_one_shared_total_read_their_own_view(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'), int_schema('v'));
        $rows = array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 4], ['k' => 'a', 'v' => 2]], $schema);
        $totals = $backend->groupedTotals();
        $sums = new TotalsByGroup(sum(ref('v')), $totals);

        $sums->aggregateGroups($rows, $backend->groupTable(['k'], $schema)->assign($rows), $backend);

        static::assertSame([3.0, 4.0], $sums->groupValues(2, $backend)->values());
        static::assertSame(
            [1.5, 4.0],
            (new TotalsByGroup(average(ref('v')), $totals))
                ->groupValues(2, $backend)
                ->values(),
        );
    }

    public function test_partials_are_each_groups_count_and_running_sum(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'), int_schema('v'));
        $rows = array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 4], ['k' => 'a', 'v' => 2]], $schema);
        $sums = new TotalsByGroup(sum(ref('v')), $backend->groupedTotals());

        $sums->aggregateGroups($rows, $backend->groupTable(['k'], $schema)->assign($rows), $backend);

        static::assertSame(
            [
                ['count' => 2, 'sum' => ['int' => 3, 'float' => null, 'compensation' => 0.0]],
                ['count' => 1, 'sum' => ['int' => 4, 'float' => null, 'compensation' => 0.0]],
            ],
            $sums->partials(2),
        );
    }

    public function test_a_batch_lacking_the_column_leaves_its_groups_without_a_total(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'), int_schema('v', nullable: true));
        $table = $backend->groupTable(['k'], $schema);
        $sums = new TotalsByGroup(sum(ref('v')), $backend->groupedTotals());
        $first = array_to_rows([['k' => 'a', 'v' => 1]], $schema);
        $lacking = array_to_rows([['k' => 'b']], schema(str_schema('k')));

        $sums->aggregateGroups($first, $table->assign($first), $backend);
        $sums->aggregateGroups($lacking, $table->assign($lacking), $backend);

        static::assertSame([1.0, null], $sums->groupValues(2, $backend)->values());
    }

    public function test_absorbing_earlier_totals_folds_them_in_front(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'), int_schema('v'));
        $earlier = new TotalsByGroup(sum(ref('v')), $backend->groupedTotals());
        $later = new TotalsByGroup(sum(ref('v')), $backend->groupedTotals());
        $rows = array_to_rows([['k' => 'a', 'v' => 5]], $schema);
        $earlier->aggregateGroups($rows, $backend->groupTable(['k'], $schema)->assign($rows), $backend);
        $later->aggregateGroups($rows, $backend->groupTable(['k'], $schema)->assign($rows), $backend);

        $later->absorbEarlier($earlier, [0 => 0], $backend);

        static::assertSame([10.0], $later->groupValues(1, $backend)->values());
    }

    public function test_it_absorbs_only_other_grouped_totals(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new TotalsByGroup(sum(ref('v')), (new PhpBackend())->groupedTotals()))->absorbEarlier(
            new CountsByGroup(null, (new PhpBackend())->groupedCounts()),
            [],
            new PhpBackend(),
        );
    }
}
