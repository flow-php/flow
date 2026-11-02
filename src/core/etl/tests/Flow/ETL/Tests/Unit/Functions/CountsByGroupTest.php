<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\CountsByGroup;
use Flow\ETL\Functions\TotalsByGroup;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class CountsByGroupTest extends FlowTestCase
{
    public function test_it_counts_non_null_values_and_a_batch_lacking_the_column_counts_zero_for_its_groups(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'), str_schema('note', nullable: true));
        $table = $backend->groupTable(['k'], $schema);
        $counts = new CountsByGroup('note', $backend->groupedCounts());
        $first = array_to_rows([['k' => 'a', 'note' => 'x'], ['k' => 'a', 'note' => null]], $schema);
        $lacking = array_to_rows([['k' => 'b']], schema(str_schema('k')));

        $counts->aggregateGroups($first, $table->assign($first), $backend);
        $counts->aggregateGroups($lacking, $table->assign($lacking), $backend);

        static::assertSame([1, 0], $counts->groupValues(2, $backend)->values());
    }

    public function test_without_a_column_it_counts_rows(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'));
        $rows = array_to_rows([['k' => 'a'], ['k' => 'b'], ['k' => 'a']], $schema);
        $counts = new CountsByGroup(null, $backend->groupedCounts());

        $counts->aggregateGroups($rows, $backend->groupTable(['k'], $schema)->assign($rows), $backend);

        static::assertSame([2, 1], $counts->groupValues(2, $backend)->values());
    }

    public function test_partials_are_each_groups_count_as_count_holds_it(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'));
        $rows = array_to_rows([['k' => 'a'], ['k' => 'b'], ['k' => 'a']], $schema);
        $counts = new CountsByGroup(null, $backend->groupedCounts());

        $counts->aggregateGroups($rows, $backend->groupTable(['k'], $schema)->assign($rows), $backend);

        static::assertSame([['count' => 2], ['count' => 1]], $counts->partials(2));
    }

    public function test_absorbing_earlier_counts_adds_them_into_the_mapped_groups(): void
    {
        $backend = new PhpBackend();
        $schema = schema(str_schema('k'));
        $earlier = new CountsByGroup(null, $backend->groupedCounts());
        $later = new CountsByGroup(null, $backend->groupedCounts());
        $first = array_to_rows([['k' => 'a'], ['k' => 'a']], $schema);
        $earlier->aggregateGroups($first, $backend->groupTable(['k'], $schema)->assign($first), $backend);
        $later->aggregateGroups($first, $backend->groupTable(['k'], $schema)->assign($first), $backend);

        $later->absorbEarlier($earlier, [0 => 1], $backend);

        static::assertSame([2, 2], $later->groupValues(2, $backend)->values());
    }

    public function test_it_absorbs_only_other_grouped_counts(): void
    {
        $this->expectException(InvalidArgumentException::class);

        (new CountsByGroup(null, (new PhpBackend())->groupedCounts()))->absorbEarlier(
            new TotalsByGroup(sum(ref('v')), (new PhpBackend())->groupedTotals()),
            [],
            new PhpBackend(),
        );
    }
}
