<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy;
use Flow\ETL\Processor\GroupByProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;

final class GroupByTest extends FlowTestCase
{
    public function test_group_by_missing_entry(): void
    {
        $groupBy = new GroupBy('type');
        $groupBy->aggregate(count());

        static::assertEquals(
            rows(
                row(str_entry('type', 'a'), int_entry('_count', 1)),
                row(null_entry('type'), int_entry('_count', 1)),
                row(str_entry('type', 'c'), int_entry('_count', 1)),
            ),
            $this->aggregate($groupBy, rows(
                row(str_entry('type', 'a')),
                row(str_entry('not-type', 'b')),
                row(str_entry('type', 'c')),
            )),
        );
    }

    public function test_group_by_with_aggregation(): void
    {
        $group = new GroupBy('type');
        $group->aggregate(sum(ref('id')));

        static::assertEquals(
            rows(
                row(int_entry('id_sum', 5), str_entry('type', 'a')),
                row(int_entry('id_sum', 2), str_entry('type', 'b')),
                row(int_entry('id_sum', 3), str_entry('type', 'c')),
                row(int_entry('id_sum', 5), str_entry('type', 'd')),
            ),
            $this->aggregate($group, rows(
                row(int_entry('id', 1), str_entry('type', 'a')),
                row(int_entry('id', 2), str_entry('type', 'b')),
                row(int_entry('id', 3), str_entry('type', 'c')),
                row(int_entry('id', 4), str_entry('type', 'a')),
                row(int_entry('id', 5), str_entry('type', 'd')),
            )),
        );
    }

    public function test_group_by_with_empty_aggregations(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Aggregations can't be empty");
        $groupBy = new GroupBy();
        $groupBy->aggregate();
    }

    public function test_group_by_with_pivoting(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        static::assertEquals(
            rows(
                row(
                    str_entry('product', 'Banana'),
                    int_entry('Canada', 2000),
                    int_entry('China', 400),
                    null_entry('Mexico'),
                    int_entry('USA', 1000),
                ),
                row(
                    str_entry('product', 'Beans'),
                    null_entry('Canada'),
                    int_entry('China', 1500),
                    int_entry('Mexico', 2000),
                    int_entry('USA', 1600),
                ),
                row(
                    str_entry('product', 'Carrots'),
                    int_entry('Canada', 2000),
                    int_entry('China', 1200),
                    null_entry('Mexico'),
                    int_entry('USA', 1500),
                ),
                row(
                    str_entry('product', 'Orange'),
                    null_entry('Canada'),
                    int_entry('China', 4000),
                    null_entry('Mexico'),
                    int_entry('USA', 4000),
                ),
            ),
            $this->aggregate($group, rows(
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
            ))->sortBy(ref('product')),
        );
    }

    public function test_group_by_with_pivoting_with_null_pivot_column(): void
    {
        $group = new GroupBy(ref('product'));
        $group->aggregate(sum(ref('amount')));
        $group->pivot(ref('country'));

        static::assertEquals(
            rows(
                row(str_entry('product', 'Apple'), null_entry('USA')),
                row(str_entry('product', 'Banana'), int_entry('USA', 1000)),
            ),
            $this->aggregate($group, rows(
                row(str_entry('product', 'Banana'), str_entry('country', 'USA'), int_entry('amount', 1000)),
                row(str_entry('product', 'Apple'), str_entry('country', null), int_entry('amount', 400)),
            ))->sortBy(ref('product')),
        );
    }

    private function aggregate(GroupBy $groupBy, Rows $input): Rows
    {
        $result = new Rows();

        $output = (new GroupByProcessor($groupBy))->process((static fn() => yield $input)(), flow_context(config()));

        /** @var Rows $batch */
        foreach ($output as $batch) {
            $result = $result->merge($batch);
        }

        return $result;
    }
}
