<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;
use Flow\ETL\Config;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class GroupByTest extends TestCase
{
    public function test_group_by_array_entry() : void
    {
        $this->expectExceptionMessage('Grouping by non scalar values is not supported, given: array');
        $this->expectException(RuntimeException::class);

        $groupBy = new GroupBy('array');

        $groupBy->group(new Rows(
            Row::create(array_entry('array', [1, 2, 3])),
            Row::create(array_entry('array', [1, 2, 3])),
            Row::create(array_entry('array', [4, 5, 6]))
        ));
    }

    public function test_group_by_missing_entry() : void
    {
        $groupBy = new GroupBy('type');

        $groupBy->group(new Rows(
            Row::create(str_entry('type', 'a')),
            Row::create(str_entry('not-type', 'b')),
            Row::create(str_entry('type', 'c'))
        ));

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('type', 'a')),
                Row::create(null_entry('type')),
                Row::create(str_entry('type', 'c'))
            ),
            $groupBy->result(new FlowContext(Config::default()))
        );
    }

    public function test_group_by_with_aggregation() : void
    {
        $group = (new GroupBy('type'));

        $group->aggregate(sum(ref('id')));
        $group->group(new Rows(
            Row::create(int_entry('id', 1), str_entry('type', 'a')),
            Row::create(int_entry('id', 2), str_entry('type', 'b')),
            Row::create(int_entry('id', 3), str_entry('type', 'c')),
            Row::create(int_entry('id', 4), str_entry('type', 'a')),
            Row::create(int_entry('id', 5), str_entry('type', 'd'))
        ));

        $this->assertEquals(
            new Rows(
                Row::create(int_entry('id_sum', 5), str_entry('type', 'a')),
                Row::create(int_entry('id_sum', 2), str_entry('type', 'b')),
                Row::create(int_entry('id_sum', 3), str_entry('type', 'c')),
                Row::create(int_entry('id_sum', 5), str_entry('type', 'd')),
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
        $rows = new Rows(
            Row::create(str_entry('product', 'Banana'), int_entry('amount', 1000), str_entry('country', 'USA')),
            Row::create(str_entry('product', 'Carrots'), int_entry('amount', 1500), str_entry('country', 'USA')),
            Row::create(str_entry('product', 'Beans'), int_entry('amount', 1600), str_entry('country', 'USA')),
            Row::create(str_entry('product', 'Orange'), int_entry('amount', 2000), str_entry('country', 'USA')),
            Row::create(str_entry('product', 'Orange'), int_entry('amount', 2000), str_entry('country', 'USA')),
            Row::create(str_entry('product', 'Banana'), int_entry('amount', 400), str_entry('country', 'China')),
            Row::create(str_entry('product', 'Carrots'), int_entry('amount', 1200), str_entry('country', 'China')),
            Row::create(str_entry('product', 'Beans'), int_entry('amount', 1500), str_entry('country', 'China')),
            Row::create(str_entry('product', 'Orange'), int_entry('amount', 4000), str_entry('country', 'China')),
            Row::create(str_entry('product', 'Banana'), int_entry('amount', 2000), str_entry('country', 'Canada')),
            Row::create(str_entry('product', 'Carrots'), int_entry('amount', 2000), str_entry('country', 'Canada')),
            Row::create(str_entry('product', 'Beans'), int_entry('amount', 2000), str_entry('country', 'Mexico')),
        );

        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        $group->group($rows);

        $this->assertEquals(
            new Rows(
                Row::create(str_entry('product', 'Banana'), int_entry('Canada', 2000), int_entry('China', 400), null_entry('Mexico'), int_entry('USA', 1000)),
                Row::create(str_entry('product', 'Beans'), null_entry('Canada'), int_entry('China', 1500), int_entry('Mexico', 2000), int_entry('USA', 1600)),
                Row::create(str_entry('product', 'Carrots'), int_entry('Canada', 2000), int_entry('China', 1200), null_entry('Mexico'), int_entry('USA', 1500)),
                Row::create(str_entry('product', 'Orange'), null_entry('Canada'), int_entry('China', 4000), null_entry('Mexico'), int_entry('USA', 4000)),
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
