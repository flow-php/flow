<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{count, df, from_array};
use Flow\ETL\Tests\FlowTestCase;

final class CountTest extends FlowTestCase
{
    public function test_count_aggregation() : void
    {
        self::assertEquals(
            [
                ['_count' => 3],
            ],
            df()
                ->read(from_array([
                    ['a' => 1],
                    ['a' => 2],
                    ['a' => 3],
                ]))
                ->aggregate(count())
                ->fetch()
                ->toArray()
        );
    }

    public function test_count_with_group_by() : void
    {
        self::assertEquals(
            [
                ['group' => 'a', '_count' => 3],
                ['group' => 'b', '_count' => 2],
            ],
            df()
                ->read(from_array([
                    ['id' => 1, 'group' => 'a'],
                    ['id' => 2, 'group' => 'a'],
                    ['id' => 3, 'group' => 'a'],
                    ['id' => 4, 'group' => 'b'],
                    ['id' => 5, 'group' => 'b'],
                ]))
                ->groupBy('group')
                ->aggregate(count())
                ->fetch()
                ->toArray()
        );
    }

    public function test_count_with_group_by_on_multiple_columns() : void
    {
        self::assertEquals(
            [
                ['group' => 'a', '_count' => 2, 'subgroup' => 'x'],
                ['group' => 'a', '_count' => 1, 'subgroup' => 'y'],
                ['group' => 'b', '_count' => 1, 'subgroup' => 'x'],
                ['group' => 'b', '_count' => 1, 'subgroup' => 'y'],
            ],
            df()
                ->read(from_array([
                    ['id' => 1, 'group' => 'a', 'subgroup' => 'x'],
                    ['id' => 2, 'group' => 'a', 'subgroup' => 'y'],
                    ['id' => 3, 'group' => 'a', 'subgroup' => 'x'],
                    ['id' => 4, 'group' => 'b', 'subgroup' => 'x'],
                    ['id' => 5, 'group' => 'b', 'subgroup' => 'y'],
                ]))
                ->groupBy('group', 'subgroup')
                ->aggregate(count())
                ->fetch()
                ->toArray()
        );
    }
}
