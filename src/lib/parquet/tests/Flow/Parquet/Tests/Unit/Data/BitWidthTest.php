<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\BitWidth;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class BitWidthTest extends TestCase
{
    #[TestWith([0, 0])]
    #[TestWith([1, 1])]
    #[TestWith([2, 2])]
    #[TestWith([255, 8])]
    #[TestWith([256, 9])]
    #[TestWith([4503599627370496, 53])]
    #[TestWith([18014398509481984, 55])]
    #[TestWith([18014398509481987, 55])]
    #[TestWith([4611686018427387904, 63])]
    #[TestWith([PHP_INT_MAX, 63])]
    public function test_calculate_is_the_exact_bit_length(int $value, int $bits): void
    {
        static::assertSame($bits, BitWidth::calculate($value));
    }

    public function test_from_array_of_a_negative_value_is_64_bits(): void
    {
        static::assertSame(64, BitWidth::fromArray([3, -1]));
    }
}
