<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data;

use Flow\Parquet\ParquetFile\Data\DeltaCalculator;
use PHPUnit\Framework\TestCase;

final class DeltaCalculatorTest extends TestCase
{
    private DeltaCalculator $calculator;

    protected function setUp() : void
    {
        $this->calculator = new DeltaCalculator();
    }

    public function test_calculate_delta_extreme_negative_jump() : void
    {
        // Test a large negative jump that would underflow in naive subtraction
        $delta = $this->calculator->calculateDelta(PHP_INT_MAX - 1000, PHP_INT_MIN);

        // This should wrap correctly
        self::assertSame(1001, $delta);
    }

    public function test_calculate_delta_extreme_positive_jump() : void
    {
        // Test a large positive jump that would overflow in naive subtraction
        $delta = $this->calculator->calculateDelta(PHP_INT_MIN + 1000, PHP_INT_MAX);

        // This should wrap correctly
        self::assertSame(-1001, $delta);
    }

    public function test_calculate_delta_handles_max_to_min_transition() : void
    {
        // Direct transition from max to min
        $delta = $this->calculator->calculateDelta(PHP_INT_MAX, PHP_INT_MIN);

        // PHP_INT_MIN - PHP_INT_MAX should equal 1 in 2's complement wrapping
        self::assertSame(1, $delta);
    }

    public function test_calculate_delta_handles_min_to_max_transition() : void
    {
        // Direct transition from min to max
        $delta = $this->calculator->calculateDelta(PHP_INT_MIN, PHP_INT_MAX);

        // PHP_INT_MAX - PHP_INT_MIN should equal -1 in 2's complement wrapping
        self::assertSame(-1, $delta);
    }

    public function test_calculate_delta_sequential_large_values() : void
    {
        // Test with sequential large values where deltas are small
        $base = PHP_INT_MAX - 5;
        $values = [$base, $base + 1, $base + 2, $base + 3];

        $deltas = $this->calculator->calculateDeltas($values);

        self::assertSame([1, 1, 1], $deltas);
    }

    public function test_calculate_delta_with_negative_values() : void
    {
        self::assertSame(-5, $this->calculator->calculateDelta(-10, -15));
        self::assertSame(5, $this->calculator->calculateDelta(-15, -10));
        self::assertSame(10, $this->calculator->calculateDelta(-5, 5));
        self::assertSame(-10, $this->calculator->calculateDelta(5, -5));
    }

    public function test_calculate_delta_with_normal_values() : void
    {
        self::assertSame(5, $this->calculator->calculateDelta(10, 15));
        self::assertSame(-5, $this->calculator->calculateDelta(15, 10));
        self::assertSame(0, $this->calculator->calculateDelta(100, 100));
        self::assertSame(1, $this->calculator->calculateDelta(0, 1));
        self::assertSame(-1, $this->calculator->calculateDelta(1, 0));
    }

    public function test_calculate_delta_with_overflow_wrapping() : void
    {
        // Test overflow from PHP_INT_MAX to PHP_INT_MIN + some value
        // This should wrap around using 2's complement arithmetic
        $delta = $this->calculator->calculateDelta(PHP_INT_MAX, PHP_INT_MIN + 1000);

        // The delta calculation should wrap around in 2's complement
        // Going from PHP_INT_MAX to PHP_INT_MIN+1000 represents a forward wrap

        self::assertSame(1001, $delta);
    }

    public function test_calculate_delta_with_underflow_wrapping() : void
    {
        // Test underflow from PHP_INT_MIN to PHP_INT_MAX - some value
        // This should wrap around using 2's complement arithmetic
        $delta = $this->calculator->calculateDelta(PHP_INT_MIN, PHP_INT_MAX - 1000);

        // Going from PHP_INT_MIN to PHP_INT_MAX-1000 represents a backward wrap
        self::assertSame(-1001, $delta);
    }

    public function test_calculate_delta_with_zero() : void
    {
        self::assertSame(10, $this->calculator->calculateDelta(0, 10));
        self::assertSame(-10, $this->calculator->calculateDelta(0, -10));
        self::assertSame(-5, $this->calculator->calculateDelta(5, 0));
        self::assertSame(5, $this->calculator->calculateDelta(-5, 0));
    }

    public function test_calculate_deltas_preserves_correct_delta_values() : void
    {
        // Test that delta calculation works correctly with extreme values
        $originalValues = [PHP_INT_MAX, PHP_INT_MIN + 1000, PHP_INT_MAX - 500];

        $deltas = $this->calculator->calculateDeltas($originalValues);

        // Verify the deltas are calculated correctly
        self::assertCount(2, $deltas);
        self::assertSame(1001, $deltas[0]); // (PHP_INT_MIN + 1000) - PHP_INT_MAX with wrapping
        self::assertSame(-1501, $deltas[1]); // (PHP_INT_MAX - 500) - (PHP_INT_MIN + 1000) with wrapping
    }

    public function test_calculate_deltas_with_array() : void
    {
        $values = [10, 15, 12, 20, 18];
        $expected = [5, -3, 8, -2]; // 15-10, 12-15, 20-12, 18-20

        $deltas = $this->calculator->calculateDeltas($values);

        self::assertSame($expected, $deltas);
    }

    public function test_calculate_deltas_with_empty_array() : void
    {
        $deltas = $this->calculator->calculateDeltas([]);

        self::assertSame([], $deltas);
    }

    public function test_calculate_deltas_with_single_value() : void
    {
        $deltas = $this->calculator->calculateDeltas([42]);

        self::assertSame([], $deltas);
    }

    public function test_reconstruct_values_from_deltas() : void
    {
        $originalValues = [10, 15, 12, 20, 18];
        $deltas = $this->calculator->calculateDeltas($originalValues);

        $reconstructed = $this->calculator->reconstructValues($originalValues[0], $deltas);

        self::assertSame($originalValues, $reconstructed);
    }

    public function test_reconstruct_values_with_overflow_wrapping() : void
    {
        // Test with values that cause overflow in delta calculation
        $originalValues = [PHP_INT_MAX, PHP_INT_MIN + 1000, PHP_INT_MAX - 500];
        $deltas = $this->calculator->calculateDeltas($originalValues);

        $reconstructed = $this->calculator->reconstructValues($originalValues[0], $deltas);

        self::assertSame($originalValues, $reconstructed);
    }

    public function test_roundtrip_preserves_values_with_extreme_cases() : void
    {
        // Test various extreme cases to ensure roundtrip integrity
        $testCases = [
            [PHP_INT_MAX, PHP_INT_MIN],
            [PHP_INT_MIN, PHP_INT_MAX],
            [0, PHP_INT_MAX, PHP_INT_MIN, 0],
            [PHP_INT_MAX - 1000, PHP_INT_MAX, PHP_INT_MIN, PHP_INT_MIN + 1000],
        ];

        foreach ($testCases as $originalValues) {
            $deltas = $this->calculator->calculateDeltas($originalValues);
            $reconstructed = $this->calculator->reconstructValues($originalValues[0], $deltas);

            self::assertSame(
                $originalValues,
                $reconstructed,
                'Roundtrip failed for values: ' . implode(', ', $originalValues)
            );
        }
    }

    public function test_calculate_relative_delta_with_normal_values() : void
    {
        self::assertSame(0, $this->calculator->calculateRelativeDelta(10, 10));
        self::assertSame(5, $this->calculator->calculateRelativeDelta(15, 10));
        self::assertSame(-5, $this->calculator->calculateRelativeDelta(5, 10));
        self::assertSame(100, $this->calculator->calculateRelativeDelta(150, 50));
        self::assertSame(-50, $this->calculator->calculateRelativeDelta(-25, 25));
    }

    public function test_calculate_relative_delta_with_zero_min_delta() : void
    {
        self::assertSame(10, $this->calculator->calculateRelativeDelta(10, 0));
        self::assertSame(-10, $this->calculator->calculateRelativeDelta(-10, 0));
        self::assertSame(0, $this->calculator->calculateRelativeDelta(0, 0));
        self::assertSame(PHP_INT_MAX, $this->calculator->calculateRelativeDelta(PHP_INT_MAX, 0));
        self::assertSame(PHP_INT_MIN, $this->calculator->calculateRelativeDelta(PHP_INT_MIN, 0));
    }

    public function test_calculate_relative_delta_with_negative_min_delta() : void
    {
        // When minDelta is negative, the relative delta should increase
        self::assertSame(20, $this->calculator->calculateRelativeDelta(10, -10));
        self::assertSame(10, $this->calculator->calculateRelativeDelta(0, -10));
        self::assertSame(0, $this->calculator->calculateRelativeDelta(-10, -10));
        self::assertSame(-5, $this->calculator->calculateRelativeDelta(-15, -10));
    }

    public function test_calculate_relative_delta_with_positive_min_delta() : void
    {
        // When minDelta is positive, the relative delta should decrease
        self::assertSame(0, $this->calculator->calculateRelativeDelta(10, 10));
        self::assertSame(-10, $this->calculator->calculateRelativeDelta(0, 10));
        self::assertSame(-20, $this->calculator->calculateRelativeDelta(-10, 10));
        self::assertSame(5, $this->calculator->calculateRelativeDelta(15, 10));
    }

    public function test_calculate_relative_delta_with_extreme_overflow_positive() : void
    {
        // Test case where delta - minDelta would overflow in naive subtraction
        // Using large positive delta and large negative minDelta
        $delta = PHP_INT_MAX;
        $minDelta = PHP_INT_MIN + 1000;
        
        $relativeDelta = $this->calculator->calculateRelativeDelta($delta, $minDelta);
        
        // This should wrap correctly according to 2's complement arithmetic
        // PHP_INT_MAX - (PHP_INT_MIN + 1000) = PHP_INT_MAX - PHP_INT_MIN - 1000
        // which wraps to -1001 in 2's complement
        self::assertSame(-1001, $relativeDelta);
    }

    public function test_calculate_relative_delta_with_extreme_overflow_negative() : void
    {
        // Test case where delta - minDelta would underflow in naive subtraction
        // Using large negative delta and large positive minDelta
        $delta = PHP_INT_MIN;
        $minDelta = PHP_INT_MAX - 1000;
        
        $relativeDelta = $this->calculator->calculateRelativeDelta($delta, $minDelta);
        
        // This should wrap correctly according to 2's complement arithmetic
        // PHP_INT_MIN - (PHP_INT_MAX - 1000) = PHP_INT_MIN - PHP_INT_MAX + 1000
        // which wraps to 1001 in 2's complement
        self::assertSame(1001, $relativeDelta);
    }

    public function test_calculate_relative_delta_max_to_min_transition() : void
    {
        // Direct transition case: max delta minus min delta
        $delta = PHP_INT_MAX;
        $minDelta = PHP_INT_MIN;
        
        $relativeDelta = $this->calculator->calculateRelativeDelta($delta, $minDelta);
        
        // PHP_INT_MAX - PHP_INT_MIN should wrap to -1 in 2's complement
        self::assertSame(-1, $relativeDelta);
    }

    public function test_calculate_relative_delta_min_to_max_transition() : void
    {
        // Reverse transition case: min delta minus max delta
        $delta = PHP_INT_MIN;
        $minDelta = PHP_INT_MAX;
        
        $relativeDelta = $this->calculator->calculateRelativeDelta($delta, $minDelta);
        
        // PHP_INT_MIN - PHP_INT_MAX should wrap to 1 in 2's complement
        self::assertSame(1, $relativeDelta);
    }

    public function test_calculate_relative_delta_near_boundary_values() : void
    {
        // Test values near the boundary of overflow/underflow
        $testCases = [
            ['delta' => PHP_INT_MAX - 100, 'minDelta' => -100, 'expected' => PHP_INT_MAX],
            ['delta' => PHP_INT_MIN + 100, 'minDelta' => 100, 'expected' => PHP_INT_MIN],
            ['delta' => PHP_INT_MAX - 1, 'minDelta' => -1, 'expected' => PHP_INT_MAX],
            ['delta' => PHP_INT_MIN + 1, 'minDelta' => 1, 'expected' => PHP_INT_MIN],
        ];

        foreach ($testCases as $case) {
            $result = $this->calculator->calculateRelativeDelta($case['delta'], $case['minDelta']);
            self::assertSame(
                $case['expected'],
                $result,
                "Failed for delta={$case['delta']}, minDelta={$case['minDelta']}"
            );
        }
    }

    public function test_calculate_relative_delta_large_values_no_overflow() : void
    {
        // Test large values that don't cause overflow
        $testCases = [
            ['delta' => 1000000000, 'minDelta' => 500000000, 'expected' => 500000000],
            ['delta' => -1000000000, 'minDelta' => -2000000000, 'expected' => 1000000000],
            ['delta' => 2147483647, 'minDelta' => 1073741824, 'expected' => 1073741823], // Large 32-bit values
            ['delta' => -2147483648, 'minDelta' => -1073741824, 'expected' => -1073741824],
        ];

        foreach ($testCases as $case) {
            $result = $this->calculator->calculateRelativeDelta($case['delta'], $case['minDelta']);
            self::assertSame(
                $case['expected'],
                $result,
                "Failed for delta={$case['delta']}, minDelta={$case['minDelta']}"
            );
        }
    }

    public function test_calculate_relative_delta_symmetry_with_calculate_delta() : void
    {
        // Test that relative delta calculation is consistent with delta calculation
        // If we have delta = current - previous, then relativeDelta = delta - minDelta
        // should be equivalent to directly calculating (current - previous) - minDelta
        
        $testCases = [
            ['previous' => 100, 'current' => 150, 'minDelta' => 25],
            ['previous' => -100, 'current' => 200, 'minDelta' => -50],
            ['previous' => 0, 'current' => 1000, 'minDelta' => 500],
            ['previous' => 1000, 'current' => 0, 'minDelta' => -200],
        ];

        foreach ($testCases as $case) {
            $delta = $this->calculator->calculateDelta($case['previous'], $case['current']);
            $relativeDelta = $this->calculator->calculateRelativeDelta($delta, $case['minDelta']);
            
            // Verify that this is equivalent to direct calculation (when no overflow occurs)
            $directCalculation = ($case['current'] - $case['previous']) - $case['minDelta'];
            
            self::assertSame(
                $directCalculation,
                $relativeDelta,
                "Symmetry test failed for previous={$case['previous']}, current={$case['current']}, minDelta={$case['minDelta']}"
            );
        }
    }

    public function test_calculate_relative_delta_edge_cases_with_overflow() : void
    {
        // Test specific edge cases that are known to cause overflow
        $testCases = [
            // Case 1: Large positive delta, large negative minDelta (should cause positive overflow)
            [
                'delta' => PHP_INT_MAX - 1000,
                'minDelta' => PHP_INT_MIN + 500,
                'description' => 'Large positive delta with large negative minDelta'
            ],
            // Case 2: Large negative delta, large positive minDelta (should cause negative overflow)
            [
                'delta' => PHP_INT_MIN + 1000,
                'minDelta' => PHP_INT_MAX - 500,
                'description' => 'Large negative delta with large positive minDelta'
            ],
            // Case 3: Maximum possible difference
            [
                'delta' => PHP_INT_MAX,
                'minDelta' => PHP_INT_MIN,
                'description' => 'Maximum delta minus minimum delta'
            ],
            // Case 4: Minimum possible difference
            [
                'delta' => PHP_INT_MIN,
                'minDelta' => PHP_INT_MAX,
                'description' => 'Minimum delta minus maximum delta'
            ],
        ];

        foreach ($testCases as $case) {
            // The test should not throw an exception and should return a valid integer result
            $result = $this->calculator->calculateRelativeDelta($case['delta'], $case['minDelta']);
            
            // Verify the result is within valid integer bounds
            self::assertGreaterThanOrEqual(PHP_INT_MIN, $result, "Result should be >= PHP_INT_MIN for: {$case['description']}");
            self::assertLessThanOrEqual(PHP_INT_MAX, $result, "Result should be <= PHP_INT_MAX for: {$case['description']}");
        }
    }
}
