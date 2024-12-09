<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\{int_entry, ref, row, rows, str_entry, sum};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\{Config, FlowContext, GroupBy};
use PHPUnit\Framework\TestCase;

final class GroupByTest extends TestCase
{
    public function test_group_by_missing_entry() : void
    {
        $groupBy = new GroupBy('type');

        $groupBy->group(rows(
            row(str_entry('type', 'a')),
            row(str_entry('not-type', 'b')),
            row(str_entry('type', 'c'))
        ));

        self::assertEquals(
            rows(
                row(str_entry('type', 'a')),
                row(str_entry('type', null)),
                row(str_entry('type', 'c'))
            ),
            $groupBy->result(new FlowContext(Config::default()))
        );
    }

    public function test_group_by_with_aggregation() : void
    {
        $group = (new GroupBy('type'));

        $group->aggregate(sum(ref('id')));
        $group->group(rows(
            row(int_entry('id', 1), str_entry('type', 'a')),
            row(int_entry('id', 2), str_entry('type', 'b')),
            row(int_entry('id', 3), str_entry('type', 'c')),
            row(int_entry('id', 4), str_entry('type', 'a')),
            row(int_entry('id', 5), str_entry('type', 'd'))
        ));

        self::assertEquals(
            rows(
                row(int_entry('id_sum', 5), str_entry('type', 'a')),
                row(int_entry('id_sum', 2), str_entry('type', 'b')),
                row(int_entry('id_sum', 3), str_entry('type', 'c')),
                row(int_entry('id_sum', 5), str_entry('type', 'd')),
            ),
            $group->result(new FlowContext(Config::default()))
        );
    }

    public function test_group_by_with_empty_aggregations() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Aggregations can't be empty");
        $groupBy = new GroupBy();
        $groupBy->aggregate();
    }

    public function test_group_by_with_pivoting() : void
    {
        $rows = rows(
            row(str_entry('product', 'Banana'), int_entry('amount', 1000), str_entry('country', 'USA')),
            row(str_entry('product', 'Carrots'), int_entry('amount', 1500), str_entry('country', 'USA')),
            row(str_entry('product', 'Beans'), int_entry('amount', 1600), str_entry('country', 'USA')),
            row(str_entry('product', 'Orange'), int_entry('amount', 2000), str_entry('country', 'USA')),
            row(str_entry('product', 'Orange'), int_entry('amount', 2000), str_entry('country', 'USA')),
            row(str_entry('product', 'Banana'), int_entry('amount', 400), str_entry('country', 'China')),
            row(str_entry('product', 'Carrots'), int_entry('amount', 1200), str_entry('country', 'China')),
            row(str_entry('product', 'Beans'), int_entry('amount', 1500), str_entry('country', 'China')),
            row(str_entry('product', 'Orange'), int_entry('amount', 4000), str_entry('country', 'China')),
            row(str_entry('product', 'Banana'), int_entry('amount', 2000), str_entry('country', 'Canada')),
            row(str_entry('product', 'Carrots'), int_entry('amount', 2000), str_entry('country', 'Canada')),
            row(str_entry('product', 'Beans'), int_entry('amount', 2000), str_entry('country', 'Mexico')),
        );

        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        $group->group($rows);

        self::assertEquals(
            rows(
                row(str_entry('product', 'Banana'), int_entry('Canada', 2000), int_entry('China', 400), str_entry('Mexico', null), int_entry('USA', 1000)),
                row(str_entry('product', 'Beans'), str_entry('Canada', null), int_entry('China', 1500), int_entry('Mexico', 2000), int_entry('USA', 1600)),
                row(str_entry('product', 'Carrots'), int_entry('Canada', 2000), int_entry('China', 1200), str_entry('Mexico', null), int_entry('USA', 1500)),
                row(str_entry('product', 'Orange'), str_entry('Canada', null), int_entry('China', 4000), str_entry('Mexico', null), int_entry('USA', 4000)),
            ),
            $group->result(new FlowContext(Config::default()))->sortBy(ref('product'))
        );
    }

    public function test_group_by_with_pivoting_with_null_pivot_column() : void
    {
        $rows = rows(
            row(str_entry('product', 'Banana'), str_entry('country', 'USA'), int_entry('amount', 1000)),
            row(str_entry('product', 'Apple'), str_entry('country', null), int_entry('amount', 400)),
        );

        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        $group->group($rows);

        self::assertEquals(
            rows(
                row(str_entry('product', 'Apple'), str_entry('USA', null)),
                row(str_entry('product', 'Banana'), int_entry('USA', 1000)),
            ),
            $group->result(new FlowContext(Config::default()))->sortBy(ref('product'))
        );
    }

    public function test_pivot_with_more_than_one_group_by_entry() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Pivot requires exactly one entry reference in group by, given: 2');

        $group = (new GroupBy('type', 'id'));

        $group->aggregate(sum(ref('id')));
        $group->pivot(ref('id'));
    }
}
