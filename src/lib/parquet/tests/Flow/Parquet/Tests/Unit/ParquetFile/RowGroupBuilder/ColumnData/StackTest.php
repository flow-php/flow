<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroupBuilder\ColumnData;

use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\Stack;
use PHPUnit\Framework\TestCase;

final class StackTest extends TestCase
{
    public function test_max_repetition_0() : void
    {
        $stack = new Stack(0);

        $stack->push(0, 1);
        $stack->push(0, 2);
        $stack->push(0, 3);

        self::assertEquals(
            [1, 2, 3],
            $stack->dump()
        );
    }

    public function test_pushing_value_on_a_level_1() : void
    {
        $stack = new Stack(1);

        $stack->push(0, [1]);
        $stack->push(1, [2]);
        $stack->push(1, [3]);

        self::assertEquals(
            [
                [1, 2, 3],
            ],
            $stack->dump()
        );
    }

    public function test_pushing_value_on_a_level_higher_than_max_level() : void
    {
        $stack = new Stack(0);

        $this->expectExceptionMessage('Given level "1"  is greater than max level, "0"');
        $stack->push(1, 1);
    }

    public function test_pushing_value_on_different_lists_on_level_1() : void
    {
        $stack = new Stack(1);

        $stack->push(0, [1]);
        $stack->push(1, [2]);
        $stack->push(0, [3]);
        $stack->push(1, [4]);
        $stack->push(1, [5]);
        $stack->push(0, [6]);

        self::assertEquals(
            [
                [1, 2], [3, 4, 5], [6],
            ],
            $stack->dump()
        );
    }

    public function test_pushing_value_on_different_lists_on_level_2() : void
    {
        $stack = new Stack(2);

        $stack->push(0, [[1]]);
        $stack->push(2, [[2]]);
        $stack->push(2, [[3]]);
        $stack->push(1, [[4]]);
        $stack->push(2, [[5]]);
        $stack->push(2, [[6]]);
        $stack->push(0, [[7]]);
        $stack->push(1, [[8]]);
        $stack->push(2, [[9]]);

        self::assertEquals(
            [
                [
                    [1, 2, 3],
                    [4, 5, 6],
                ],
                [
                    [7],
                    [8, 9],
                ],
            ],
            $stack->dump()
        );
    }

    public function test_pushing_value_on_different_lists_on_level_2_01() : void
    {
        $stack = new Stack(2);

        $stack->push(0, [[1]]);
        $stack->push(1, [[2]]);
        $stack->push(2, [[3]]);
        $stack->push(0, [[1]]);
        $stack->push(1, [[2]]);
        $stack->push(2, [[3]]);

        self::assertEquals(
            [
                [
                    [1],
                    [2, 3],
                ],
                [
                    [1],
                    [2, 3],
                ],
            ],
            $stack->dump()
        );
    }

    public function test_pushing_value_on_different_lists_on_level_3() : void
    {
        $stack = new Stack(3);

        $stack->push(0, [[[1]]]);
        $stack->push(1, [[[2]]]);
        $stack->push(1, [[[3]]]);

        self::assertEquals(
            [
                [
                    [
                        [1],
                    ],
                    [
                        [2],
                    ],
                    [
                        [3],
                    ],
                ],
            ],
            $stack->dump()
        );
    }

    public function test_pushing_value_on_different_lists_on_level_4() : void
    {
        $stack = new Stack(4);

        $stack->push(0, [[[[1]]]]);
        $stack->push(2, [[[[2]]]]);

        self::assertEquals(
            [
                [
                    [
                        [
                            [1],
                        ],
                        [
                            [2],
                        ],
                    ],
                ],
            ],
            $stack->dump()
        );
    }
}
