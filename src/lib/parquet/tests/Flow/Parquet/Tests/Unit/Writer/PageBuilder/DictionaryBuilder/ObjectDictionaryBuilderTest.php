<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\PageBuilder\DictionaryBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer\PageBuilder\Dictionary;
use Flow\Parquet\Writer\PageBuilder\DictionaryBuilder\ObjectDictionaryBuilder;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ObjectDictionaryBuilderTest extends TestCase
{
    private ObjectDictionaryBuilder $builder;

    public static function object_value_types_provider(): \Generator
    {
        $date1 = new \DateTimeImmutable('2023-01-01');
        $date2 = new \DateTimeImmutable('2023-01-02');
        $date3 = new \DateTimeImmutable('2023-01-03');

        yield 'datetime objects' => [
            [$date1, $date2, $date3, $date2, $date1],
            [$date1, $date2, $date3],
            [0,      1,      2,      1,      0],
        ];

        yield 'datetime objects with nulls' => [
            [$date1, null, $date2, null, $date1],
            [$date1, $date2],
            [0, 1, 0],
        ];

        $interval1 = new \DateInterval('P1D');
        $interval2 = new \DateInterval('P1Y');

        yield 'date intervals' => [
            [$interval1, $interval2, $interval1, $interval2],
            [$interval1, $interval2],
            [0,          1,          0,          1],
        ];

        yield 'mixed objects' => [
            [$date1, $interval1, $date2, $interval1, $date1],
            [$date1, $interval1, $date2],
            [0,      1,          2,      1,          0],
        ];
    }

    protected function setUp(): void
    {
        $this->builder = new ObjectDictionaryBuilder();
    }

    public function test_all_null_values_returns_empty_dictionary(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $data = new WriteFlatColumnValues($column, values: [null, null, null]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertSame([], $result->dictionary);
        static::assertSame([], $result->indices);
    }

    public function test_alternating_object_pattern(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $date1 = new \DateTimeImmutable('2023-01-01');
        $date2 = new \DateTimeImmutable('2023-01-02');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$date1, $date2, $date1, $date2, $date1, $date2]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(2, $result->dictionary);
        static::assertSame([0, 1, 0, 1, 0, 1], $result->indices);
        static::assertEquals($date1, $result->dictionary[0]);
        static::assertEquals($date2, $result->dictionary[1]);
    }

    public function test_date_interval_negative_values(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $interval1 = new \DateInterval('P1D');
        $interval2 = new \DateInterval('P1D');
        $interval2->invert = 1;
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$interval1, $interval2, $interval1]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(2, $result->dictionary);
        static::assertSame([0, 1, 0], $result->indices);
        static::assertEquals($interval1, $result->dictionary[0]);
        static::assertEquals($interval2, $result->dictionary[1]);
    }

    public function test_date_interval_objects(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $interval1 = new \DateInterval('P1D');
        $interval2 = new \DateInterval('P1Y');
        $interval3 = new \DateInterval('PT1H');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [
            $interval1,
            $interval2,
            $interval3,
            $interval2,
            $interval1,
        ]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(3, $result->dictionary);
        static::assertSame([0, 1, 2, 1, 0], $result->indices);
        static::assertEquals($interval1, $result->dictionary[0]);
        static::assertEquals($interval2, $result->dictionary[1]);
        static::assertEquals($interval3, $result->dictionary[2]);
    }

    public function test_date_interval_with_complex_values(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $interval1 = new \DateInterval('P1Y2M3DT4H5M6S');
        $interval2 = new \DateInterval('P10DT2H30M');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$interval1, $interval2, $interval1]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(2, $result->dictionary);
        static::assertSame([0, 1, 0], $result->indices);
        static::assertEquals($interval1, $result->dictionary[0]);
        static::assertEquals($interval2, $result->dictionary[1]);
    }

    public function test_datetime_immutable_with_different_formats(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $date1 = new \DateTimeImmutable('2023-01-01');
        $date2 = new \DateTimeImmutable('01/01/2023');
        $date3 = new \DateTimeImmutable('January 1, 2023');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$date1, $date2, $date3]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(1, $result->dictionary);
        static::assertSame([0, 0, 0], $result->indices);
        static::assertEquals($date1, $result->dictionary[0]);
    }

    public function test_datetime_immutable_with_different_timezones(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $dateUTC = new \DateTimeImmutable('2023-01-01 12:00:00', new \DateTimeZone('UTC'));
        $dateNY = new \DateTimeImmutable('2023-01-01 12:00:00', new \DateTimeZone('America/New_York'));
        $dateTokyo = new \DateTimeImmutable('2023-01-01 12:00:00', new \DateTimeZone('Asia/Tokyo'));
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$dateUTC, $dateNY, $dateTokyo, $dateNY, $dateUTC]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(3, $result->dictionary);
        static::assertSame([0, 1, 2, 1, 0], $result->indices);
        static::assertEquals($dateUTC, $result->dictionary[0]);
        static::assertEquals($dateNY, $result->dictionary[1]);
        static::assertEquals($dateTokyo, $result->dictionary[2]);
    }

    public function test_datetime_immutable_with_microseconds(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $date1 = new \DateTimeImmutable('2023-01-01 12:00:00.123456');
        $date2 = new \DateTimeImmutable('2023-01-01 12:00:00.654321');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$date1, $date2, $date1]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(2, $result->dictionary);
        static::assertSame([0, 1, 0], $result->indices);
        static::assertEquals($date1, $result->dictionary[0]);
        static::assertEquals($date2, $result->dictionary[1]);
    }

    /**
     * @param array<null|object> $values
     * @param array<object> $expectedDictionary
     * @param array<int> $expectedIndices
     */
    #[DataProvider('object_value_types_provider')]
    public function test_different_object_types(array $values, array $expectedDictionary, array $expectedIndices): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: $values);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(count($expectedDictionary), $result->dictionary);
        static::assertSame($expectedIndices, $result->indices);

        for ($i = 0; $i < count($expectedDictionary); $i++) {
            static::assertEquals($expectedDictionary[$i], $result->dictionary[$i]);
        }
    }

    public function test_duplicate_datetime_immutable_creates_indexed_dictionary(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $date1 = new \DateTimeImmutable('2023-01-01');
        $date2 = new \DateTimeImmutable('2023-01-02');
        $date3 = new \DateTimeImmutable('2023-01-03');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$date1, $date2, $date3, $date2, $date1, $date3]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(3, $result->dictionary);
        static::assertSame([0, 1, 2, 1, 0, 2], $result->indices);
        static::assertEquals($date1, $result->dictionary[0]);
        static::assertEquals($date2, $result->dictionary[1]);
        static::assertEquals($date3, $result->dictionary[2]);
    }

    public function test_empty_data_returns_empty_dictionary(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $data = new WriteFlatColumnValues($column, values: []);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertSame([], $result->dictionary);
        static::assertSame([], $result->indices);
    }

    public function test_large_number_of_duplicate_objects(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $dateTime = new \DateTimeImmutable('2023-01-01 12:00:00');
        $values = array_fill(0, 1000, $dateTime);
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: $values);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(1, $result->dictionary);
        static::assertSame(array_fill(0, 1000, 0), $result->indices);
        static::assertEquals($dateTime, $result->dictionary[0]);
    }

    public function test_large_number_of_unique_objects(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $values = [];

        for ($i = 0; $i < 50; $i++) {
            $values[] = new \DateTimeImmutable(sprintf('2023-01-01 12:00:%02d', $i));
        }
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: $values);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(50, $result->dictionary);
        static::assertSame(array_keys($values), $result->indices);

        for ($i = 0; $i < 50; $i++) {
            static::assertEquals($values[$i], $result->dictionary[$i]);
        }
    }

    public function test_maintains_first_occurrence_order(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $date1 = new \DateTimeImmutable('2023-01-05');
        $date2 = new \DateTimeImmutable('2023-01-02');
        $date3 = new \DateTimeImmutable('2023-01-08');
        $date4 = new \DateTimeImmutable('2023-01-01');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$date1, $date2, $date3, $date4, $date2, $date1]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(4, $result->dictionary);
        static::assertSame([0, 1, 2, 3, 1, 0], $result->indices);
        static::assertEquals($date1, $result->dictionary[0]);
        static::assertEquals($date2, $result->dictionary[1]);
        static::assertEquals($date3, $result->dictionary[2]);
        static::assertEquals($date4, $result->dictionary[3]);
    }

    public function test_mixed_datetime_immutable_and_date_interval(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $dateTime = new \DateTimeImmutable('2023-01-01 12:00:00');
        $interval = new \DateInterval('P1D');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$dateTime, $interval, $dateTime, $interval]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(2, $result->dictionary);
        static::assertSame([0, 1, 0, 1], $result->indices);
        static::assertEquals($dateTime, $result->dictionary[0]);
        static::assertEquals($interval, $result->dictionary[1]);
    }

    public function test_mixed_nulls_and_datetime_immutable(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $date1 = new \DateTimeImmutable('2023-01-01');
        $date2 = new \DateTimeImmutable('2023-01-02');
        $date3 = new \DateTimeImmutable('2023-01-03');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [null, $date1, null, $date2, $date1, null, $date3, $date2]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(3, $result->dictionary);
        static::assertSame([0, 1, 0, 2, 1], $result->indices);
        static::assertEquals($date1, $result->dictionary[0]);
        static::assertEquals($date2, $result->dictionary[1]);
        static::assertEquals($date3, $result->dictionary[2]);
    }

    public function test_multiple_unique_datetime_immutable_creates_ordered_dictionary(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $date1 = new \DateTimeImmutable('2023-01-01');
        $date2 = new \DateTimeImmutable('2023-01-02');
        $date3 = new \DateTimeImmutable('2023-01-03');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$date1, $date2, $date3]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(3, $result->dictionary);
        static::assertSame([0, 1, 2], $result->indices);
        static::assertEquals($date1, $result->dictionary[0]);
        static::assertEquals($date2, $result->dictionary[1]);
        static::assertEquals($date3, $result->dictionary[2]);
    }

    public function test_serialization_distinguishes_different_objects(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $dateTime1 = new \DateTimeImmutable('2023-01-01 12:00:00');
        $dateTime2 = new \DateTimeImmutable('2023-01-01 12:00:01');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$dateTime1, $dateTime2]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(2, $result->dictionary);
        static::assertSame([0, 1], $result->indices);
        static::assertEquals($dateTime1, $result->dictionary[0]);
        static::assertEquals($dateTime2, $result->dictionary[1]);
    }

    public function test_serialization_preserves_object_equality(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $dateTime1 = new \DateTimeImmutable('2023-01-01 12:00:00');
        $dateTime2 = new \DateTimeImmutable('2023-01-01 12:00:00');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$dateTime1, $dateTime2]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(1, $result->dictionary);
        static::assertSame([0, 0], $result->indices);
        static::assertEquals($dateTime1, $result->dictionary[0]);
        static::assertEquals($dateTime2, $result->dictionary[0]);
    }

    public function test_single_datetime_immutable_creates_single_entry_dictionary(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $dateTime = new \DateTimeImmutable('2023-01-01 12:00:00');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [$dateTime]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(1, $result->dictionary);
        static::assertSame([0], $result->indices);
        static::assertEquals($dateTime, $result->dictionary[0]);
    }

    public function test_single_value_with_nulls_creates_single_entry_dictionary(): void
    {
        $column = new FlatColumn('test_column', PhysicalType::INT64);
        $dateTime = new \DateTimeImmutable('2023-01-01 12:00:00');
        /** @phpstan-ignore-next-line */
        $data = new WriteFlatColumnValues($column, values: [null, $dateTime, null, $dateTime, null]);

        $result = $this->builder->build($data);

        static::assertInstanceOf(Dictionary::class, $result);
        static::assertCount(1, $result->dictionary);
        static::assertSame([0, 0], $result->indices);
        static::assertEquals($dateTime, $result->dictionary[0]);
    }
}
