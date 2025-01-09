<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use function Flow\Parquet\array_merge_recursive;
use PHPUnit\Framework\TestCase;

final class FunctionsTest extends TestCase
{
    public function test_array_merge_recursive() : void
    {
        self::assertSame(
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
            array_merge_recursive(
                [
                    'members' => [
                        0 => ['addresses' => [0 => ['street' => 'Street_2_0_0']]],
                    ],
                ],
                [
                    'members' => [
                        0 => ['addresses' => [1 => ['street' => 'Street_2_0_1']]],
                        1 => ['addresses' => [0 => ['street' => 'Street_2_1_0']]],
                    ],
                ]
            )
        );
    }
}
