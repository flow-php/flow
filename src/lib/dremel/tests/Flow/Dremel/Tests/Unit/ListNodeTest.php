<?php

declare(strict_types=1);

namespace Flow\Dremel\Tests\Unit;

use Flow\Dremel\ListNode;
use PHPUnit\Framework\TestCase;

final class ListNodeTest extends TestCase
{
    public function test_creating_list_node_with_repetition_3() : void
    {
        $this->assertsame(
            [[[]]],
            (new ListNode(3))->value()
        );
    }

    public function test_push_by_2_levels() : void
    {
        self::assertSame(
            [
                [
                    [
                        [1],
                    ],
                    [
                        [2],
                    ],
                ],
                [
                    [
                        [3],
                    ],
                    [
                        [4, 5],
                    ],
                ],
            ],
            (new ListNode(4))
                ->push(1, level: 4)
                ->push(2, level: 2)
                ->push(3, level: 1)
                ->push(4, level: 2)
                ->push(5, level: 4)
                ->value(),
        );
    }

    public function test_push_to_level_3_then_2_then_1() : void
    {
        self::assertSame(
            [
                [
                    [
                        0 => 1,
                    ],
                    [
                        0 => 2,
                    ],
                ],
                [
                    [
                        0 => 3,
                    ],
                ],
            ],
            (new ListNode(3))->push(1, 3)->push(2, 2)->push(3, 1)->value(),
        );
    }

    public function test_push_to_level_3_then_2_then_1_then_1_again() : void
    {
        self::assertSame(
            [
                [
                    [
                        0 => 1,
                    ],
                    [
                        0 => 2,
                    ],
                ],
                [
                    [
                        0 => 3,
                    ],
                ],
                [
                    [
                        0 => 4,
                    ],
                ],
            ],
            (new ListNode(3))->push(1, 3)->push(2, 2)->push(3, 1)->push(4, 1)->value(),
        );
    }

    public function test_push_to_level_3_then_2_then_1_then_2() : void
    {
        self::assertSame(
            [
                [
                    [
                        0 => 1,
                    ],
                    [
                        0 => 2,
                    ],
                ],
                [
                    [
                        0 => 3,
                    ],
                    [
                        0 => 4,
                    ],
                ],
            ],
            (new ListNode(3))->push(1, 3)->push(2, 2)->push(3, 1)->push(4, 2)->value(),
        );
    }

    public function test_push_to_level_3_then_2_then_1_then_3() : void
    {
        self::assertSame(
            [
                [
                    [
                        0 => 1,
                    ],
                    [
                        0 => 2,
                    ],
                ],
                [
                    [
                        0 => 3,
                        1 => 4,
                    ],
                ],
            ],
            (new ListNode(3))->push(1, 3)->push(2, 2)->push(3, 1)->push(4, 3)->value(),
        );
    }

    public function test_push_value_highest_level_then_lower_and_higher() : void
    {
        self::assertSame(
            [
                [
                    [
                        0 => 1,
                    ],
                    [
                        0 => 2,
                    ],
                    [
                        0 => 3,
                    ],
                ],
            ],
            (new ListNode(3))->push(1, 3)->push(2, 2)->push(3, 2)->value(),
        );
    }

    public function test_push_value_to_highest_level() : void
    {
        self::assertSame(
            [
                [
                    [
                        0 => 1,
                    ],
                    [
                        0 => 2,
                        1 => 3,
                    ],
                ],
            ],
            (new ListNode(3))->push(value: 1, level: 3)->push(value: 2, level: 2)->push(value: 3, level: 3)->value(),
        );
    }

    public function test_push_value_to_lower_level() : void
    {
        self::assertSame(
            [
                [
                    [
                        0 => 1,
                    ],
                    [
                        0 => 2,
                    ],
                ],
            ],
            (new ListNode(3))->push(value: 1, level: 3)->push(value: 2, level: 2)->value(),
        );
    }

    public function test_push_value_to_specific_level() : void
    {
        self::assertSame(
            [
                [
                    [
                        0 => 1,
                    ],
                ],
            ],
            (new ListNode(3))->push(value: 1, level: 3)->value(),
        );
    }
}
