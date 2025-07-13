<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\ZigZag;
use PHPUnit\Framework\TestCase;

final class ZigZagLargeValuesTest extends TestCase
{
    public function test_zigzag_large_positive_values() : void
    {
        $zigzag = new ZigZag();

        $problematicValues = [
            7595602394150081560,
            5962760613797072767,
            6865028960044616023,
            PHP_INT_MAX,
            PHP_INT_MAX - 1000,
        ];

        $failingValues = [
            7595602394150081560,
            PHP_INT_MAX - 100,
        ];

        foreach (\array_merge($failingValues, $problematicValues) as $value) {
            $encoded = $zigzag->encode($value);
            $decoded = $zigzag->decode($encoded);
            self::assertSame($value, $decoded, "ZigZag should roundtrip correctly for value: {$value}");
        }
    }

    public function test_zigzag_php_int_max_variations() : void
    {
        $zigzag = new ZigZag();

        $values = [
            PHP_INT_MAX,
            PHP_INT_MAX - 1,
            PHP_INT_MAX - 100,
            PHP_INT_MAX - 1000,
            PHP_INT_MAX - 10000,
        ];

        foreach ($values as $value) {
            $encoded = $zigzag->encode($value);
            $decoded = $zigzag->decode($encoded);

            self::assertSame($value, $decoded, "ZigZag roundtrip failed for value: {$value}");
        }
    }
}
