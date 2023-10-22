<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit;

use function Flow\Parquet\array_combine_recursive;
use PHPUnit\Framework\TestCase;

final class FunctionsTest extends TestCase
{
    public function test_array_combine_recursive() : void
    {
        $this->assertSame(
            [
                [
                    [
                        ['street' => 'Street_2_0_1'],
                    ],
                ],
            ],
            array_combine_recursive(
                [
                    'members' => [
                        ['addresses' => ['street']],
                    ],
                ],
                [
                    'members' => [
                        ['addresses' => ['Street_2_0_1']],
                        ['addresses' => ['Street_2_1_0']],
                    ],
                ]
            )
        );
    }
}
