<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use Flow\ETL\Config;
use Flow\ETL\DSL\Entry;
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
            Row::create(Entry::array('array', [1, 2, 3])),
            Row::create(Entry::array('array', [1, 2, 3])),
            Row::create(Entry::array('array', [4, 5, 6]))
        ));
    }

    public function test_group_by_missing_entry() : void
    {
        $groupBy = new GroupBy('type');

        $groupBy->group(new Rows(
            Row::create(Entry::string('type', 'a')),
            Row::create(Entry::string('not-type', 'b')),
            Row::create(Entry::string('type', 'c'))
        ));

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('type', 'a')),
                Row::create(Entry::null('type')),
                Row::create(Entry::string('type', 'c'))
            ),
            $groupBy->result(new FlowContext(Config::default()))
        );
    }

    public function test_group_by_with_aggregation() : void
    {
        $group = (new GroupBy('type'));

        $group->aggregate(sum(ref('id')));
        $group->group(new Rows(
            Row::create(Entry::int('id', 1), Entry::string('type', 'a')),
            Row::create(Entry::int('id', 2), Entry::string('type', 'b')),
            Row::create(Entry::int('id', 3), Entry::string('type', 'c')),
            Row::create(Entry::int('id', 4), Entry::string('type', 'a')),
            Row::create(Entry::int('id', 5), Entry::string('type', 'd'))
        ));

        $this->assertEquals(
            new Rows(
                Row::create(Entry::int('id_sum', 5), Entry::string('type', 'a')),
                Row::create(Entry::int('id_sum', 2), Entry::string('type', 'b')),
                Row::create(Entry::int('id_sum', 3), Entry::string('type', 'c')),
                Row::create(Entry::int('id_sum', 5), Entry::string('type', 'd')),
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
            Row::create(Entry::string('product', 'Banana'), Entry::int('amount', 1000), Entry::string('country', 'USA')),
            Row::create(Entry::string('product', 'Carrots'), Entry::int('amount', 1500), Entry::string('country', 'USA')),
            Row::create(Entry::string('product', 'Beans'), Entry::int('amount', 1600), Entry::string('country', 'USA')),
            Row::create(Entry::string('product', 'Orange'), Entry::int('amount', 2000), Entry::string('country', 'USA')),
            Row::create(Entry::string('product', 'Orange'), Entry::int('amount', 2000), Entry::string('country', 'USA')),
            Row::create(Entry::string('product', 'Banana'), Entry::int('amount', 400), Entry::string('country', 'China')),
            Row::create(Entry::string('product', 'Carrots'), Entry::int('amount', 1200), Entry::string('country', 'China')),
            Row::create(Entry::string('product', 'Beans'), Entry::int('amount', 1500), Entry::string('country', 'China')),
            Row::create(Entry::string('product', 'Orange'), Entry::int('amount', 4000), Entry::string('country', 'China')),
            Row::create(Entry::string('product', 'Banana'), Entry::int('amount', 2000), Entry::string('country', 'Canada')),
            Row::create(Entry::string('product', 'Carrots'), Entry::int('amount', 2000), Entry::string('country', 'Canada')),
            Row::create(Entry::string('product', 'Beans'), Entry::int('amount', 2000), Entry::string('country', 'Mexico')),
        );

        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        $group->group($rows);

        $this->assertEquals(
            new Rows(
                Row::create(Entry::string('product', 'Banana'), Entry::int('Canada', 2000), Entry::int('China', 400), Entry::null('Mexico'), Entry::int('USA', 1000)),
                Row::create(Entry::string('product', 'Beans'), Entry::null('Canada'), Entry::int('China', 1500), Entry::int('Mexico', 2000), Entry::int('USA', 1600)),
                Row::create(Entry::string('product', 'Carrots'), Entry::int('Canada', 2000), Entry::int('China', 1200), Entry::null('Mexico'), Entry::int('USA', 1500)),
                Row::create(Entry::string('product', 'Orange'), Entry::null('Canada'), Entry::int('China', 4000), Entry::null('Mexico'), Entry::int('USA', 4000)),
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
