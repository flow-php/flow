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
        $expected = (PHP_INT_MIN + 1000) - PHP_INT_MAX; // This will be 1001 when wrapped

        if ($expected > PHP_INT_MAX) {
            $expected -= (PHP_INT_MAX - PHP_INT_MIN + 1); // Wrap around
        } elseif ($expected < PHP_INT_MIN) {
            $expected += (PHP_INT_MAX - PHP_INT_MIN + 1); // Wrap around
        }

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
}
