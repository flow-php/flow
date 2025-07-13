<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\{DeltaBinaryPackedDecoder, DeltaBinaryPackedEncoder};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

/**
 * Comprehensive test for int64 delta encoding issues.
 * This test captures all the problematic int64 values and ensures they roundtrip correctly.
 */
final class DeltaBinaryPackedInt64Test extends TestCase
{
    /**
     * @return array<string, array<array<int>>>
     */
    public static function problematicInt64ValuesProvider() : array
    {
        return [
            // ✅ These should now work with ZigZag fixes
            'php_int_max_sequence' => [[PHP_INT_MAX, PHP_INT_MAX - 1000, PHP_INT_MAX - 2000]],
            'php_int_max_variations' => [[PHP_INT_MAX - 100, PHP_INT_MAX - 200, PHP_INT_MAX - 300]],
            'large_near_max_values' => [[9223372036854775000, 9223372036854774000, 9223372036854773000]],

            // ❌ These are still problematic and need further investigation
            'large_positive_values' => [[7595602394150081560, 5962760613797072767, 6865028960044616023]],
            'mixed_large_values' => [[1798933430085394339, 8845978805043041461, 6588414276814208324]],
            'failing_compression_test_sample' => [[
                7595602394150081560, // Expected, but got 7595602394150081560 ✅
                5962760613797072767, // Expected, but got 5962760613797072767 ✅
                6865028960044616023, // Expected, but got 8882641593106598231 ❌
            ]],

            // Additional test cases for comprehensive coverage
            'single_large_values' => [[7595602394150081560]],
            'two_large_values' => [[7595602394150081560, 5962760613797072767]],
            'different_arrangement_1' => [[6865028960044616023, 7595602394150081560, 5962760613797072767]],
            'different_arrangement_2' => [[5962760613797072767, 6865028960044616023, 7595602394150081560]],

            // Edge cases with specific patterns
            'large_positive_increasing' => [[5000000000000000000, 6000000000000000000, 7000000000000000000]],
            'large_positive_decreasing' => [[9000000000000000000, 8000000000000000000, 7000000000000000000]],
            'mixed_signs_large' => [[PHP_INT_MAX, -1000000000000000000, PHP_INT_MAX - 1000]],
        ];
    }

    public function test_delta_encoding_with_known_working_values() : void
    {
        // Test with values we know should work to ensure baseline functionality
        $workingValues = [
            [100, 200, 300],
            [1000000, 2000000, 3000000],
            [PHP_INT_MAX - 1000, PHP_INT_MAX - 2000, PHP_INT_MAX - 3000],
        ];

        foreach ($workingValues as $values) {
            $encoded = (new DeltaBinaryPackedEncoder())->encode($values);
            $decoded = (new DeltaBinaryPackedDecoder())->decode($encoded, count($values));

            self::assertSame(
                $values,
                $decoded,
                'Known working values should always roundtrip correctly: ' . implode(', ', $values)
            );
        }
    }

    /**
     * @param array<int> $values
     */
    #[DataProvider('problematicInt64ValuesProvider')]
    public function test_int64_delta_encoding_roundtrip(array $values) : void
    {
        $encoded = (new DeltaBinaryPackedEncoder())->encode($values);
        $decoded = (new DeltaBinaryPackedDecoder())->decode($encoded, count($values));

        $testName = $this->dataName();

        self::assertSame(
            $values,
            $decoded,
            "Int64 values should roundtrip correctly through delta encoding for test case: {$testName}"
        );
    }
}
