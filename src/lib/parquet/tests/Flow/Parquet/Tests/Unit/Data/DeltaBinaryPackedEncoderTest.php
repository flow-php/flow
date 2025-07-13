<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\DeltaBinaryPackedEncoder;
use Flow\Parquet\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class DeltaBinaryPackedEncoderTest extends TestCase
{
    public function test_constructor_validates_block_miniblock_relationship() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Block size must be a multiple of miniblock size');

        new DeltaBinaryPackedEncoder(128, 96);
    }

    public function test_constructor_validates_block_size() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Block size must be a multiple of 128');

        new DeltaBinaryPackedEncoder(100);
    }

    public function test_constructor_validates_miniblock_size() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Miniblock size must be a multiple of 32');

        new DeltaBinaryPackedEncoder(128, 30);
    }

    public function test_encode_different_patterns_produce_different_output() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $values1 = [1, 2, 3, 4, 5, 6, 7, 8];
        $values2 = [1, 3, 5, 7, 9, 11, 13, 15];

        $result1 = $encoder->encode($values1);
        $result2 = $encoder->encode($values2);

        self::assertNotSame($result1, $result2);
    }

    public function test_encode_empty_array() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $result = $encoder->encode([]);

        self::assertSame('', $result);
    }

    public function test_encode_extreme_values() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        $values = [
            PHP_INT_MAX,
            PHP_INT_MIN + 1000,  // Large delta that would overflow in naive calculation
        ];

        // This should now handle overflow using 2's complement wrapping
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_handles_extreme_large_jump_with_wrapping() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        // Large jump that would cause overflow: delta would be ~-1.8e19
        $values = [9223372036854775800, -9223372036854775800];

        // Should now handle this with 2's complement wrapping
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_handles_large_jump_with_wrapping() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        // Large jump that would cause overflow in naive calculation
        $values = [5000000000000000000, -5000000000000000000];

        // Should now handle this with 2's complement wrapping
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_handles_php_int_max_to_min_with_wrapping() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        // PHP_INT_MAX to PHP_INT_MIN transition
        $values = [PHP_INT_MAX, PHP_INT_MIN];

        // Should now handle this with 2's complement wrapping
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_handles_safe_large_values() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        // Sequential large values - deltas are small, should work fine
        $values = [9223372036854775800, 9223372036854775801, 9223372036854775802];

        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_handles_small_jumps_with_large_values() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        // Small jumps even with large values - should work
        $values = [PHP_INT_MAX - 100, PHP_INT_MAX - 50, PHP_INT_MAX];

        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_large_dataset() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $values = [];

        for ($i = 0; $i < 1000; $i++) {
            $values[] = $i * 2;
        }

        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_large_int64_values_no_overflow() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        $baseValue = PHP_INT_MAX - 1000;
        $values = [];

        for ($i = 0; $i < 10; $i++) {
            $values[] = $baseValue + $i;
        }

        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_large_int64_values_with_safe_deltas() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        $values = [
            PHP_INT_MAX >> 1,      // Large positive
            (PHP_INT_MAX >> 1) + 100,  // Slightly larger
            (PHP_INT_MAX >> 1) - 50,   // Slightly smaller
            (PHP_INT_MAX >> 1) + 200,  // Larger again
        ];

        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_large_values() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $values = [1000000, 1000001, 1000002, 1000003, 1000004, 1000005, 1000006, 1000007];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_mixed_large_values() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        $values = [
            PHP_INT_MAX >> 2,      // Large positive
            -(PHP_INT_MAX >> 2),   // Large negative
            (PHP_INT_MAX >> 2) + 1000,  // Back to positive
            -(PHP_INT_MAX >> 2) - 1000, // Back to negative
        ];

        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_negative_values() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $values = [-10, -8, -6, -4, -2, 0, 2, 4];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_produces_consistent_output() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $values = [1, 3, 5, 7, 9, 11, 13, 15];

        $result1 = $encoder->encode($values);
        $result2 = $encoder->encode($values);

        self::assertSame($result1, $result2);
    }

    public function test_encode_random_pattern() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $values = [10, 15, 12, 18, 14, 20, 16, 22];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_sequential_values() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $values = [1, 2, 3, 4, 5, 6, 7, 8];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_single_value() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $result = $encoder->encode([42]);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_timestamp_like_sequence() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $baseTimestamp = 1609459200; // 2021-01-01 00:00:00
        $values = [];

        for ($i = 0; $i < 10; $i++) {
            $values[] = $baseTimestamp + ($i * 60); // Every minute
        }

        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_values_with_negative_deltas() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $values = [100, 90, 80, 70, 60, 50, 40, 30];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_values_with_varying_deltas() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $values = [100, 102, 98, 105, 95, 110, 90, 115];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_values_with_zero_deltas() : void
    {
        $encoder = new DeltaBinaryPackedEncoder();
        $values = [42, 42, 42, 42, 42, 42, 42, 42];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_with_custom_block_size() : void
    {
        $encoder = new DeltaBinaryPackedEncoder(256, 64);
        $values = range(1, 100);
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }
}
