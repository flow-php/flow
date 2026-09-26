<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\array_merge_recursive;
use function Flow\Parquet\floor_div;

final class FunctionsTest extends TestCase
{
    public function test_array_merge_recursive(): void
    {
        static::assertSame(
            [
                'members' => [
                    0 => [
                        'addresses' => [
                            0 => ['street' => 'Street_2_0_0'],
                            1 => ['street' => 'Street_2_0_1'],
                        ],
                    ],
                    1 => [
                        'addresses' => [
                            0 => ['street' => 'Street_2_1_0'],
                        ],
                    ],
                ],
            ],
            array_merge_recursive([
                'members' => [
                    0 => ['addresses' => [0 => ['street' => 'Street_2_0_0']]],
                ],
            ], [
                'members' => [
                    0 => ['addresses' => [1 => ['street' => 'Street_2_0_1']]],
                    1 => ['addresses' => [0 => ['street' => 'Street_2_1_0']]],
                ],
            ]),
        );
    }

    #[TestWith([7, 2, 3])]
    #[TestWith([-7, 2, -4])]
    #[TestWith([-6, 2, -3])]
    #[TestWith([7, -2, -4])]
    #[TestWith([-1500, 1000, -2])]
    #[TestWith([0, 1000, 0])]
    public function test_floor_div(int $dividend, int $divisor, int $expected): void
    {
        static::assertSame($expected, floor_div($dividend, $divisor));
    }
}
