<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Function;

use function Flow\ETL\DSL\{df, from_array, last};
use PHPUnit\Framework\TestCase;

final class LastTest extends TestCase
{
    public function test_first_aggregation_with_grouping() : void
    {
        $first = df()
            ->read(
                from_array([
                    ['id' => 1, 'value' => 10, 'group' => 'A'],
                    ['id' => 2, 'value' => 20, 'group' => 'A'],
                    ['id' => 3, 'value' => 30, 'group' => 'B'],
                    ['id' => 4, 'value' => 40, 'group' => 'C'],
                    ['id' => 5, 'value' => 50, 'group' => 'B'],
                ])
            )
            ->groupBy('group')->aggregate(last('value'))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['group' => 'A', 'value_last' => 20],
                ['group' => 'B', 'value_last' => 50],
                ['group' => 'C', 'value_last' => 40],
            ],
            $first
        );
    }

    public function test_first_aggregation_with_on_aggregated_column() : void
    {
        $first = df()
            ->read(
                from_array([
                    ['id' => 1, 'value' => 10, 'group' => 'A'],
                    ['id' => 2, 'value' => 20, 'group' => 'A'],
                    ['id' => 3, 'value' => 30, 'group' => 'B'],
                    ['id' => 4, 'value' => 40, 'group' => 'C'],
                    ['id' => 5, 'value' => 50, 'group' => 'B'],
                ])
            )
            ->groupBy('value')->aggregate(last('value'))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['value' => 10, 'value_last' => 10],
                ['value' => 20, 'value_last' => 20],
                ['value' => 30, 'value_last' => 30],
                ['value' => 40, 'value_last' => 40],
                ['value' => 50, 'value_last' => 50],
            ],
            $first
        );
    }

    public function test_last_aggregation() : void
    {
        $first = df()
            ->read(
                from_array([
                    ['id' => 1, 'value' => 10],
                    ['id' => 2, 'value' => 20],
                    ['id' => 3, 'value' => 30],
                    ['id' => 4, 'value' => 40],
                    ['id' => 5, 'value' => 50],
                ])
            )
            ->aggregate(last('value'))
            ->fetch()
            ->toArray();

        self::assertSame(
            [
                ['value_last' => 50],
            ],
            $first
        );
    }
}
