<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\ZigZag;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ZigZagTest extends TestCase
{
    private ZigZag $zigzag;

    /**
     * @return array<string, array<int>>
     */
    public static function edgeCaseValuesProvider() : array
    {
        return [
            'half_int_max' => [PHP_INT_MAX >> 1],
            'negative_half_int_max' => [-(PHP_INT_MAX >> 1)],
            'quarter_int_max' => [PHP_INT_MAX >> 2],
            'negative_quarter_int_max' => [-(PHP_INT_MAX >> 2)],
        ];
    }

    /**
     * @return array<string, array<int>>
     */
    public static function extremeValuesProvider() : array
    {
        return [
            'int_max' => [PHP_INT_MAX],
            'int_min' => [PHP_INT_MIN],
            'int_max_minus_1' => [PHP_INT_MAX - 1],
            'int_min_plus_1' => [PHP_INT_MIN + 1],
            'large_positive_1' => [7595602394150081560],
            'large_positive_2' => [5962760613797072767],
            'large_negative_1' => [-7595602394150081560],
            'large_negative_2' => [-5962760613797072767],
        ];
    }

    /**
     * @return array<string, array<int>>
     */
    public static function largeValuesProvider() : array
    {
        return [
            'medium_large_positive_1' => [1000000000000000000],
            'medium_large_positive_2' => [2000000000000000000],
            'medium_large_positive_3' => [3000000000000000000],
            'medium_large_negative_1' => [-1000000000000000000],
            'medium_large_negative_2' => [-2000000000000000000],
            'medium_large_negative_3' => [-3000000000000000000],
        ];
    }

    /**
     * @return array<string, array<int>>
     */
    public static function standardMappingProvider() : array
    {
        return [
            'zero' => [0, 0],
            'positive_one' => [1, 2],
            'negative_one' => [-1, 1],
            'positive_two' => [2, 4],
            'negative_two' => [-2, 3],
            'positive_three' => [3, 6],
            'negative_three' => [-3, 5],
            'small_positive' => [100, 200],
            'small_negative' => [-100, 199],
            'medium_positive' => [1000, 2000],
            'medium_negative' => [-1000, 1999],
        ];
    }

    protected function setUp() : void
    {
        $this->zigzag = new ZigZag();
    }

    #[DataProvider('edgeCaseValuesProvider')]
    public function test_edge_case_values_roundtrip(int $value) : void
    {
        $encoded = $this->zigzag->encode($value);
        $decoded = $this->zigzag->decode($encoded);

        self::assertSame($value, $decoded, "Edge case value {$value} should roundtrip correctly");
    }

    public function test_encode_preserves_ordering_for_small_values() : void
    {
        $values = [-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        $encoded = array_map(fn ($v) => $this->zigzag->encode($v), $values);

        $expected = [19, 17, 15, 13, 11, 9, 7, 5, 3, 1, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20];

        self::assertSame($expected, $encoded);
    }

    #[DataProvider('extremeValuesProvider')]
    public function test_extreme_values_follow_zigzag_mapping(int $value) : void
    {
        $encoded = $this->zigzag->encode($value);

        self::assertIsInt($encoded, 'ZigZag encoding should always produce an integer');

        if ($value >= 0) {
            if ($value <= (PHP_INT_MAX >> 1)) {
                self::assertTrue(
                    $encoded >= 0 && ($encoded & 1) === 0,
                    "Positive value {$value} should encode to non-negative even number, got {$encoded}"
                );
            }
        } else {
            if ($value >= -(PHP_INT_MAX >> 1)) {
                self::assertTrue(
                    $encoded > 0 && ($encoded & 1) === 1,
                    "Negative value {$value} should encode to positive odd number, got {$encoded}"
                );
            }
        }
    }

    #[DataProvider('largeValuesProvider')]
    public function test_large_values_roundtrip(int $value) : void
    {
        $encoded = $this->zigzag->encode($value);
        $decoded = $this->zigzag->decode($encoded);

        self::assertSame($value, $decoded, "Value {$value} should roundtrip correctly");
    }

    /**
     * @param array<int, int> $expected
     */
    #[DataProvider('standardMappingProvider')]
    public function test_standard_zigzag_mapping(int $original, int $expected) : void
    {
        self::assertSame($expected, $this->zigzag->encode($original));
    }

    /**
     * @param array<int, int> $expected
     */
    #[DataProvider('standardMappingProvider')]
    public function test_zigzag_roundtrip(int $original, int $encoded) : void
    {
        self::assertSame($original, $this->zigzag->decode($encoded));
    }
}
