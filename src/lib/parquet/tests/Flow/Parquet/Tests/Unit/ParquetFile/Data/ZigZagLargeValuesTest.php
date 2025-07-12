<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data;

use Flow\Parquet\ParquetFile\Data\ZigZag;
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

        print "\n=== ZigZag Large Values Test ===\n";

        foreach ($problematicValues as $value) {
            print "Original: {$value}\n";

            $encoded = $zigzag->encode($value);
            print "Encoded:  {$encoded}\n";

            $decoded = $zigzag->decode($encoded);
            print "Decoded:  {$decoded}\n";

            if ($value !== $decoded) {
                print "❌ MISMATCH! Expected: {$value}, Got: {$decoded}, Diff: " . ($value - $decoded) . "\n";
            } else {
                print "✅ OK\n";
            }
            print "---\n";
        }

        // Let's see what happens with the specific failing values
        $failingValues = [
            7595602394150081560,  // Should roundtrip correctly
            PHP_INT_MAX - 100,    // Should roundtrip correctly
        ];

        foreach ($failingValues as $value) {
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

        print "\n=== ZigZag PHP_INT_MAX Variations ===\n";
        print 'PHP_INT_MAX = ' . PHP_INT_MAX . "\n";

        foreach ($values as $value) {
            $encoded = $zigzag->encode($value);
            $decoded = $zigzag->decode($encoded);

            print "Value: {$value}\n";
            print "Encoded: {$encoded}\n";
            print "Decoded: {$decoded}\n";
            print 'Match: ' . ($value === $decoded ? 'YES' : 'NO') . "\n";
            print "---\n";

            self::assertSame($value, $decoded, "ZigZag roundtrip failed for value: {$value}");
        }
    }
}
