<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Data\DeltaEncoder;
use PHPUnit\Framework\TestCase;

final class DeltaEncoderTest extends TestCase
{
    public function test_constructor_validates_block_miniblock_relationship() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Block size must be a multiple of miniblock size');

        new DeltaEncoder(128, 96);
    }

    public function test_constructor_validates_block_size() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Block size must be a multiple of 128');

        new DeltaEncoder(100);
    }

    public function test_constructor_validates_miniblock_size() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Miniblock size must be a multiple of 32');

        new DeltaEncoder(128, 30);
    }

    public function test_encode_different_patterns_produce_different_output() : void
    {
        $encoder = new DeltaEncoder();
        $values1 = [1, 2, 3, 4, 5, 6, 7, 8];
        $values2 = [1, 3, 5, 7, 9, 11, 13, 15];

        $result1 = $encoder->encode($values1);
        $result2 = $encoder->encode($values2);

        self::assertNotSame($result1, $result2);
    }

    public function test_encode_empty_array() : void
    {
        $encoder = new DeltaEncoder();
        $result = $encoder->encode([]);

        self::assertSame('', $result);
    }

    public function test_encode_large_dataset() : void
    {
        $encoder = new DeltaEncoder();
        $values = [];

        for ($i = 0; $i < 1000; $i++) {
            $values[] = $i * 2;
        }

        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_large_values() : void
    {
        $encoder = new DeltaEncoder();
        $values = [1000000, 1000001, 1000002, 1000003, 1000004, 1000005, 1000006, 1000007];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_negative_values() : void
    {
        $encoder = new DeltaEncoder();
        $values = [-10, -8, -6, -4, -2, 0, 2, 4];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_produces_consistent_output() : void
    {
        $encoder = new DeltaEncoder();
        $values = [1, 3, 5, 7, 9, 11, 13, 15];

        $result1 = $encoder->encode($values);
        $result2 = $encoder->encode($values);

        self::assertSame($result1, $result2);
    }

    public function test_encode_random_pattern() : void
    {
        $encoder = new DeltaEncoder();
        $values = [10, 15, 12, 18, 14, 20, 16, 22];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_sequential_values() : void
    {
        $encoder = new DeltaEncoder();
        $values = [1, 2, 3, 4, 5, 6, 7, 8];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_single_value() : void
    {
        $encoder = new DeltaEncoder();
        $result = $encoder->encode([42]);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_timestamp_like_sequence() : void
    {
        $encoder = new DeltaEncoder();
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
        $encoder = new DeltaEncoder();
        $values = [100, 90, 80, 70, 60, 50, 40, 30];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_values_with_varying_deltas() : void
    {
        $encoder = new DeltaEncoder();
        $values = [100, 102, 98, 105, 95, 110, 90, 115];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_values_with_zero_deltas() : void
    {
        $encoder = new DeltaEncoder();
        $values = [42, 42, 42, 42, 42, 42, 42, 42];
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }

    public function test_encode_with_custom_block_size() : void
    {
        $encoder = new DeltaEncoder(256, 64);
        $values = range(1, 100);
        $result = $encoder->encode($values);

        self::assertNotEmpty($result);
        self::assertGreaterThan(0, strlen($result));
    }
}
