<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\DeltaCalculator;
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
        $delta = $this->calculator->calculateDelta(PHP_INT_MAX - 1000, PHP_INT_MIN);

        self::assertSame(1001, $delta);
    }

    public function test_calculate_delta_extreme_positive_jump() : void
    {
        $delta = $this->calculator->calculateDelta(PHP_INT_MIN + 1000, PHP_INT_MAX);

        self::assertSame(-1001, $delta);
    }

    public function test_calculate_delta_handles_max_to_min_transition() : void
    {
        $delta = $this->calculator->calculateDelta(PHP_INT_MAX, PHP_INT_MIN);

        self::assertSame(1, $delta);
    }

    public function test_calculate_delta_handles_min_to_max_transition() : void
    {
        $delta = $this->calculator->calculateDelta(PHP_INT_MIN, PHP_INT_MAX);

        self::assertSame(-1, $delta);
    }

    public function test_calculate_delta_sequential_large_values() : void
    {
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
        self::assertSame(1001, $this->calculator->calculateDelta(PHP_INT_MAX, PHP_INT_MIN + 1000));
    }

    public function test_calculate_delta_with_underflow_wrapping() : void
    {
        self::assertSame(-1001, $this->calculator->calculateDelta(PHP_INT_MIN, PHP_INT_MAX - 1000));
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
        $originalValues = [PHP_INT_MAX, PHP_INT_MIN + 1000, PHP_INT_MAX - 500];

        $deltas = $this->calculator->calculateDeltas($originalValues);

        self::assertCount(2, $deltas);
        self::assertSame(1001, $deltas[0]);
        self::assertSame(-1501, $deltas[1]);
    }

    public function test_calculate_deltas_with_array() : void
    {
        $values = [10, 15, 12, 20, 18];
        $expected = [5, -3, 8, -2];

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

    public function test_calculate_relative_delta_edge_cases_with_overflow() : void
    {
        $testCases = [
            [
                'delta' => PHP_INT_MAX - 1000,
                'minDelta' => PHP_INT_MIN + 500,
                'description' => 'Large positive delta with large negative minDelta',
            ],
            [
                'delta' => PHP_INT_MIN + 1000,
                'minDelta' => PHP_INT_MAX - 500,
                'description' => 'Large negative delta with large positive minDelta',
            ],
            [
                'delta' => PHP_INT_MAX,
                'minDelta' => PHP_INT_MIN,
                'description' => 'Maximum delta minus minimum delta',
            ],
            [
                'delta' => PHP_INT_MIN,
                'minDelta' => PHP_INT_MAX,
                'description' => 'Minimum delta minus maximum delta',
            ],
        ];

        foreach ($testCases as $case) {
            $result = $this->calculator->calculateRelativeDelta($case['delta'], $case['minDelta']);

            self::assertGreaterThanOrEqual(PHP_INT_MIN, $result, "Result should be >= PHP_INT_MIN for: {$case['description']}");
            self::assertLessThanOrEqual(PHP_INT_MAX, $result, "Result should be <= PHP_INT_MAX for: {$case['description']}");
        }
    }

    public function test_calculate_relative_delta_large_values_no_overflow() : void
    {
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

    public function test_calculate_relative_delta_max_to_min_transition() : void
    {
        $delta = PHP_INT_MAX;
        $minDelta = PHP_INT_MIN;

        $relativeDelta = $this->calculator->calculateRelativeDelta($delta, $minDelta);

        self::assertSame(-1, $relativeDelta);
    }

    public function test_calculate_relative_delta_min_to_max_transition() : void
    {
        $delta = PHP_INT_MIN;
        $minDelta = PHP_INT_MAX;

        $relativeDelta = $this->calculator->calculateRelativeDelta($delta, $minDelta);

        self::assertSame(1, $relativeDelta);
    }

    public function test_calculate_relative_delta_near_boundary_values() : void
    {
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

    public function test_calculate_relative_delta_symmetry_with_calculate_delta() : void
    {
        $testCases = [
            ['previous' => 100, 'current' => 150, 'minDelta' => 25],
            ['previous' => -100, 'current' => 200, 'minDelta' => -50],
            ['previous' => 0, 'current' => 1000, 'minDelta' => 500],
            ['previous' => 1000, 'current' => 0, 'minDelta' => -200],
        ];

        foreach ($testCases as $case) {
            $delta = $this->calculator->calculateDelta($case['previous'], $case['current']);
            $relativeDelta = $this->calculator->calculateRelativeDelta($delta, $case['minDelta']);

            $directCalculation = ($case['current'] - $case['previous']) - $case['minDelta'];

            self::assertSame(
                $directCalculation,
                $relativeDelta,
                "Symmetry test failed for previous={$case['previous']}, current={$case['current']}, minDelta={$case['minDelta']}"
            );
        }
    }

    public function test_calculate_relative_delta_with_extreme_overflow_negative() : void
    {
        $delta = PHP_INT_MIN;
        $minDelta = PHP_INT_MAX - 1000;

        self::assertSame(1001, $this->calculator->calculateRelativeDelta($delta, $minDelta));
    }

    public function test_calculate_relative_delta_with_extreme_overflow_positive() : void
    {
        $delta = PHP_INT_MAX;
        $minDelta = PHP_INT_MIN + 1000;

        self::assertSame(-1001, $this->calculator->calculateRelativeDelta($delta, $minDelta));
    }

    public function test_calculate_relative_delta_with_negative_min_delta() : void
    {
        self::assertSame(20, $this->calculator->calculateRelativeDelta(10, -10));
        self::assertSame(10, $this->calculator->calculateRelativeDelta(0, -10));
        self::assertSame(0, $this->calculator->calculateRelativeDelta(-10, -10));
        self::assertSame(-5, $this->calculator->calculateRelativeDelta(-15, -10));
    }

    public function test_calculate_relative_delta_with_normal_values() : void
    {
        self::assertSame(0, $this->calculator->calculateRelativeDelta(10, 10));
        self::assertSame(5, $this->calculator->calculateRelativeDelta(15, 10));
        self::assertSame(-5, $this->calculator->calculateRelativeDelta(5, 10));
        self::assertSame(100, $this->calculator->calculateRelativeDelta(150, 50));
        self::assertSame(-50, $this->calculator->calculateRelativeDelta(-25, 25));
    }

    public function test_calculate_relative_delta_with_positive_min_delta() : void
    {
        // When minDelta is positive, the relative delta should decrease
        self::assertSame(0, $this->calculator->calculateRelativeDelta(10, 10));
        self::assertSame(-10, $this->calculator->calculateRelativeDelta(0, 10));
        self::assertSame(-20, $this->calculator->calculateRelativeDelta(-10, 10));
        self::assertSame(5, $this->calculator->calculateRelativeDelta(15, 10));
    }

    public function test_calculate_relative_delta_with_zero_min_delta() : void
    {
        self::assertSame(10, $this->calculator->calculateRelativeDelta(10, 0));
        self::assertSame(-10, $this->calculator->calculateRelativeDelta(-10, 0));
        self::assertSame(0, $this->calculator->calculateRelativeDelta(0, 0));
        self::assertSame(PHP_INT_MAX, $this->calculator->calculateRelativeDelta(PHP_INT_MAX, 0));
        self::assertSame(PHP_INT_MIN, $this->calculator->calculateRelativeDelta(PHP_INT_MIN, 0));
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
        $originalValues = [PHP_INT_MAX, PHP_INT_MIN + 1000, PHP_INT_MAX - 500];
        $deltas = $this->calculator->calculateDeltas($originalValues);

        $reconstructed = $this->calculator->reconstructValues($originalValues[0], $deltas);

        self::assertSame($originalValues, $reconstructed);
    }

    public function test_roundtrip_preserves_values_with_extreme_cases() : void
    {
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
