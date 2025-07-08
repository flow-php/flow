<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn};
use Flow\Parquet\Writer\StatisticsCounter;
use PHPUnit\Framework\TestCase;

final class StatisticsCounterTest extends TestCase
{
    public static function array_values_provider() : \Generator
    {
        yield 'empty array' => [[], 1, 0];
        yield 'single string array' => [['hello'], 1, 5];
        yield 'multiple strings array' => [['hello', 'world'], 2, 10];
        yield 'nested array' => [['hello', ['nested', 'array']], 3, 16];
    }

    public static function comparison_values_provider() : \Generator
    {
        yield 'integers' => [[5, 1, 10, 3], 1, 10];
        yield 'strings' => [['zebra', 'apple', 'banana'], 'apple', 'zebra'];
        yield 'floats' => [[3.14, 1.41, 2.71], 1.41, 3.14];
        yield 'booleans' => [[true, false, true], false, true];
        yield 'single value' => [[42], 42, 42];
    }

    public static function edge_case_values_provider() : \Generator
    {
        yield 'zero integer' => [0, 1, 0];
        yield 'zero float' => [0.0, 1, 0];
        yield 'empty string' => ['', 1, 0];
        yield 'false boolean' => [false, 1, 0];
        yield 'array with null' => [[null], 1, 0];
        yield 'array with mixed nulls' => [[null, 'value', null], 3, 0];
    }

    public static function simple_values_provider() : \Generator
    {
        yield 'string value' => ['hello', 5];
        yield 'integer value' => [42, 0];
        yield 'float value' => [3.14, 0];
        yield 'boolean true' => [true, 0];
        yield 'boolean false' => [false, 0];
    }

    /**
     * @dataProvider array_values_provider
     *
     * @param array<int|string> $value
     */
    public function test_add_array_values(array $value, int $expectedValuesCount, int $expectedStringLength) : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        $statistics->add($value);

        self::assertSame(0, $statistics->nullCount());
        self::assertSame($expectedValuesCount, $statistics->valuesCount());
        self::assertSame($expectedValuesCount, $statistics->notNullCount());
    }

    public function test_add_duplicate_values_counts_correctly() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        $statistics->add('hello');
        $statistics->add('hello');
        $statistics->add('world');
        $statistics->add('hello');

        self::assertSame(0, $statistics->nullCount());
        self::assertSame(4, $statistics->valuesCount());
        self::assertSame(4, $statistics->notNullCount());
        self::assertSame('hello', $statistics->min());
        self::assertSame('world', $statistics->max());
    }

    public function test_add_multiple_values_tracks_min_max() : void
    {
        $column = FlatColumn::int32('test_column');
        $statistics = new StatisticsCounter($column);

        $statistics->add(5);
        $statistics->add(1);
        $statistics->add(10);
        $statistics->add(3);

        self::assertSame(0, $statistics->nullCount());
        self::assertSame(4, $statistics->valuesCount());
        self::assertSame(4, $statistics->notNullCount());
        self::assertSame(1, $statistics->min());
        self::assertSame(10, $statistics->max());
    }

    public function test_add_multiple_values_with_nulls() : void
    {
        $column = FlatColumn::int32('test_column');
        $statistics = new StatisticsCounter($column);

        $statistics->add(5);
        $statistics->add(null);
        $statistics->add(10);
        $statistics->add(null);
        $statistics->add(3);

        self::assertSame(2, $statistics->nullCount());
        self::assertSame(5, $statistics->valuesCount());
        self::assertSame(3, $statistics->notNullCount());
        self::assertSame(3, $statistics->min());
        self::assertSame(10, $statistics->max());
    }

    public function test_add_null_value() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        $statistics->add(null);

        self::assertSame(1, $statistics->nullCount());
        self::assertSame(1, $statistics->valuesCount());
        self::assertSame(0, $statistics->notNullCount());
        self::assertNull($statistics->min());
        self::assertNull($statistics->max());
    }

    public function test_add_object_value() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);
        $object = new \stdClass();
        $object->name = 'test';

        $statistics->add($object);

        self::assertSame(0, $statistics->nullCount());
        self::assertSame(1, $statistics->valuesCount());
        self::assertSame(1, $statistics->notNullCount());
        self::assertSame($object, $statistics->min());
        self::assertSame($object, $statistics->max());
    }

    /**
     * @dataProvider simple_values_provider
     */
    public function test_add_simple_values(string|int|float|bool $value, int $expectedStringLength) : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        $statistics->add($value);

        self::assertSame(0, $statistics->nullCount());
        self::assertSame(1, $statistics->valuesCount());
        self::assertSame(1, $statistics->notNullCount());
        self::assertSame($value, $statistics->min());
        self::assertSame($value, $statistics->max());
    }

    public function test_array_flattening() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        /** @var array<null|bool|float|int|object|string> $nestedArray */
        $nestedArray = [
            'level1',
            [
                'level2a',
                [
                    'level3a',
                    'level3b',
                ],
                'level2b',
            ],
            'level1b',
        ];

        $statistics->add($nestedArray);

        self::assertSame(6, $statistics->valuesCount());
        self::assertSame('level1', $statistics->min());
        self::assertSame('level3b', $statistics->max());
    }

    public function test_constructor_initializes_with_empty_values() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        self::assertSame(0, $statistics->nullCount());
        self::assertSame(0, $statistics->valuesCount());
        self::assertSame(0, $statistics->notNullCount());
        self::assertNull($statistics->min());
        self::assertNull($statistics->max());
    }

    /**
     * @dataProvider edge_case_values_provider
     *
     * @param null|array<null|bool|float|int|object|string>|bool|float|int|string $value
     */
    public function test_edge_cases(array|string|int|float|bool|null $value, int $expectedValuesCount, int $expectedNullCount) : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        $statistics->add($value);

        self::assertSame($expectedNullCount, $statistics->nullCount());
        self::assertSame($expectedValuesCount, $statistics->valuesCount());
    }

    public function test_merge_preserves_original_statistics() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics1 = new StatisticsCounter($column);
        $statistics2 = new StatisticsCounter($column);

        $statistics1->add('hello');
        $statistics1->add(null);

        $statistics2->add('world');

        $originalNullCount = $statistics1->nullCount();
        $originalValuesCount = $statistics1->valuesCount();

        $merged = $statistics1->merge($statistics2);

        self::assertSame($originalNullCount, $statistics1->nullCount());
        self::assertSame($originalValuesCount, $statistics1->valuesCount());
        self::assertNotSame($statistics1, $merged);
    }

    public function test_merge_with_both_empty_counters() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics1 = new StatisticsCounter($column);
        $statistics2 = new StatisticsCounter($column);

        $merged = $statistics1->merge($statistics2);

        self::assertSame(0, $merged->nullCount());
        self::assertSame(0, $merged->valuesCount());
        self::assertSame(0, $merged->notNullCount());
        self::assertNull($merged->min());
        self::assertNull($merged->max());
    }

    public function test_merge_with_different_column_throws_exception() : void
    {
        $column1 = FlatColumn::string('column1');
        $column2 = FlatColumn::string('column2');
        $statistics1 = new StatisticsCounter($column1);
        $statistics2 = new StatisticsCounter($column2);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cannot merge statistics for different columns.');

        $statistics1->merge($statistics2);
    }

    public function test_merge_with_different_min_max_values() : void
    {
        $column = FlatColumn::int32('test_column');
        $statistics1 = new StatisticsCounter($column);
        $statistics2 = new StatisticsCounter($column);

        $statistics1->add(5);
        $statistics1->add(10);
        $statistics1->add(15);

        $statistics2->add(1);
        $statistics2->add(20);
        $statistics2->add(8);

        $merged = $statistics1->merge($statistics2);

        self::assertSame(1, $merged->min());
        self::assertSame(20, $merged->max());
        self::assertSame(6, $merged->valuesCount());
        self::assertSame(0, $merged->nullCount());
    }

    public function test_merge_with_different_null_counts() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics1 = new StatisticsCounter($column);
        $statistics2 = new StatisticsCounter($column);

        $statistics1->add('hello');
        $statistics1->add(null);
        $statistics1->add(null);
        $statistics1->add('world');

        $statistics2->add('foo');
        $statistics2->add(null);

        $merged = $statistics1->merge($statistics2);

        self::assertSame(3, $merged->nullCount());
        self::assertSame(6, $merged->valuesCount());
        self::assertSame(3, $merged->notNullCount());
    }

    public function test_merge_with_different_values_counts() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics1 = new StatisticsCounter($column);
        $statistics2 = new StatisticsCounter($column);

        $statistics1->add('a');
        $statistics1->add('b');

        $statistics2->add('c');
        $statistics2->add('d');
        $statistics2->add('e');
        $statistics2->add('f');

        $merged = $statistics1->merge($statistics2);

        self::assertSame(0, $merged->nullCount());
        self::assertSame(6, $merged->valuesCount());
        self::assertSame(6, $merged->notNullCount());
    }

    public function test_merge_with_null_only_counters() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics1 = new StatisticsCounter($column);
        $statistics2 = new StatisticsCounter($column);

        $statistics1->add(null);
        $statistics1->add(null);

        $statistics2->add(null);

        $merged = $statistics1->merge($statistics2);

        self::assertSame(3, $merged->nullCount());
        self::assertSame(3, $merged->valuesCount());
        self::assertSame(0, $merged->notNullCount());
        self::assertNull($merged->min());
        self::assertNull($merged->max());
    }

    public function test_merge_with_one_empty_counter() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics1 = new StatisticsCounter($column);
        $statistics2 = new StatisticsCounter($column);

        $statistics1->add('hello');
        $statistics1->add(null);
        $statistics1->add('world');

        $merged = $statistics1->merge($statistics2);

        self::assertSame(1, $merged->nullCount());
        self::assertSame(3, $merged->valuesCount());
        self::assertSame(2, $merged->notNullCount());
        self::assertSame('hello', $merged->min());
        self::assertSame('world', $merged->max());
    }

    public function test_merge_with_one_null_only_counter() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics1 = new StatisticsCounter($column);
        $statistics2 = new StatisticsCounter($column);

        $statistics1->add('hello');
        $statistics1->add('world');

        $statistics2->add(null);
        $statistics2->add(null);

        $merged = $statistics1->merge($statistics2);

        self::assertSame(2, $merged->nullCount());
        self::assertSame(4, $merged->valuesCount());
        self::assertSame(2, $merged->notNullCount());
        self::assertSame('hello', $merged->min());
        self::assertSame('world', $merged->max());
    }

    public function test_merge_with_same_column() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics1 = new StatisticsCounter($column);
        $statistics2 = new StatisticsCounter($column);

        $statistics1->add('hello');
        $statistics1->add(null);
        $statistics1->add('world');

        $statistics2->add('foo');
        $statistics2->add('bar');
        $statistics2->add(null);

        $merged = $statistics1->merge($statistics2);

        self::assertSame(2, $merged->nullCount());
        self::assertSame(6, $merged->valuesCount());
        self::assertSame(4, $merged->notNullCount());
    }

    /**
     * @dataProvider comparison_values_provider
     *
     * @param array<bool|float|int|string> $values
     */
    public function test_min_max_with_different_types(array $values, mixed $expectedMin, mixed $expectedMax) : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        foreach ($values as $value) {
            /** @var null|array<null|bool|float|int|object|string>|bool|float|int|object|string $value */
            $statistics->add($value);
        }

        self::assertSame($expectedMin, $statistics->min());
        self::assertSame($expectedMax, $statistics->max());
    }

    public function test_not_null_count() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        $statistics->add('hello');
        $statistics->add(null);
        $statistics->add('world');
        $statistics->add(null);
        $statistics->add(['a', 'b']);

        self::assertSame(6, $statistics->valuesCount());
        self::assertSame(2, $statistics->nullCount());
        self::assertSame(4, $statistics->notNullCount());
    }

    public function test_reset() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        $statistics->add('hello');
        $statistics->add(null);
        $statistics->add('world');

        self::assertSame(1, $statistics->nullCount());
        self::assertSame(3, $statistics->valuesCount());

        $statistics->reset();

        self::assertSame(0, $statistics->nullCount());
        self::assertSame(0, $statistics->valuesCount());
        self::assertSame(0, $statistics->notNullCount());
        self::assertNull($statistics->min());
        self::assertNull($statistics->max());
    }

    public function test_values_count_calculation_with_arrays() : void
    {
        $column = FlatColumn::string('test_column');
        $statistics = new StatisticsCounter($column);

        $statistics->add(['a', 'b', 'c']);
        $statistics->add([]);
        $statistics->add(['d']);

        self::assertSame(5, $statistics->valuesCount());
        self::assertSame(0, $statistics->nullCount());
        self::assertSame(5, $statistics->notNullCount());
    }
}
