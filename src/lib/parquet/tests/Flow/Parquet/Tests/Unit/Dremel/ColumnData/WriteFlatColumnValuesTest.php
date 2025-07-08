<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Dremel\ColumnData;

use Faker\Factory;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Dremel\{DremelShredder};
use Flow\Parquet\Dremel\Validator\ColumnDataValidator;
use Flow\Parquet\Dremel\WriteColumnData;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, ListElement, NestedColumn};
use PHPUnit\Framework\TestCase;

final class WriteFlatColumnValuesTest extends TestCase
{
    public function test_flat_column() : void
    {
        $data = new WriteFlatColumnValues(FlatColumn::int32('int32'), repetitionLevels: [0, 0, 0], definitionLevels: [0, 1, 1], values: [2, 3]);

        self::assertSame(1, $data->nullCount());
        self::assertSame(3, $data->rowsCount());
    }

    public function test_list() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new WriteFlatColumnValues($schema->columnsFlat()[0], repetitionLevels: [0, 1, 0], definitionLevels: [0, 3, 3], values: [2, 3]);

        self::assertSame(1, $data->nullCount());
        self::assertSame(2, $data->rowsCount());
    }

    public function test_skip_rows_bug_with_repeated_values_in_same_row() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new WriteFlatColumnValues(
            $schema->columnsFlat()[0],
            repetitionLevels: [0, 1, 1, 0, 1],
            definitionLevels: [3, 3, 3, 3, 3],
            values: [1, 2, 3, 4, 5]
        );

        self::assertSame(2, $data->rowsCount());

        $skipped = $data->skipRows(1);

        self::assertSame(1, $skipped->rowsCount());
        self::assertSame([4, 5], $skipped->values());
        self::assertSame([3, 3], $skipped->definitionLevels());
        self::assertSame([0, 1], $skipped->repetitionLevels());
    }

    public function test_skip_rows_with_struct_containing_list_of_strings() : void
    {
        $schema = Schema::with(NestedColumn::struct('struct', [
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));

        $faker = Factory::create();
        $rows = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'struct' => [
                    'list_of_string' => $i % 2 === 0
                        ? \array_map(
                            static fn ($i) => $faker->text(10),
                            \range(1, 3)
                        )
                        : null,
                ],
            ],
        ], \range(1, 10)));

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize($options = Options::default()));

        $flatColumnData = WriteColumnData::initialize($schema->get('struct'));

        foreach ($rows as $row) {
            foreach ($dremel->shred($schema->get('struct'), $row)->flatValues() as $nextFlatValues) {
                $flatColumnData->addValues($nextFlatValues);
            }
        }

        $flatColumnValues = $flatColumnData->values('struct.list_of_string.list.element');

        self::assertSame(10, $flatColumnValues->rowsCount());

        $skippedResult = $flatColumnValues->skipRows(3);

        self::assertSame(7, $skippedResult->rowsCount(), 'Should have 7 rows after skipping 3 rows');
    }

    public function test_skipping_rows_in_flat_column() : void
    {
        $data = new WriteFlatColumnValues(
            FlatColumn::int32('int32'),
            repetitionLevels: [0, 0, 0, 0, 0, 0, 0],
            definitionLevels: [1, 1, 1, 1, 1, 1, 1],
            values: [1, 2, 3, 4, 5, 6, 7]
        );

        $skipped = $data->skipRows(2);

        self::assertSame(5, $skipped->rowsCount());
        self::assertSame([3, 4, 5, 6, 7], $skipped->values());
        self::assertSame([1, 1, 1, 1, 1], $skipped->definitionLevels());
        self::assertSame([0, 0, 0, 0, 0], $skipped->repetitionLevels());
    }

    public function test_skipping_rows_in_list() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new WriteFlatColumnValues(
            $schema->columnsFlat()[0],
            repetitionLevels: [0, 1, 0, 0, 0, 1],
            definitionLevels: [3, 3, 3, 2, 3, 3],
            values: [1, 2, 3, 4, 5]
        );

        self::assertSame(4, $data->rowsCount());

        $skipped = $data->skipRows(2);

        self::assertSame(2, $skipped->rowsCount());
        self::assertSame([4, 5], $skipped->values());
        self::assertSame([2, 3, 3], $skipped->definitionLevels());
        self::assertSame([0, 0, 1], $skipped->repetitionLevels());
    }

    public function test_skipping_rows_in_list_with_multi_elements() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new WriteFlatColumnValues(
            $schema->columnsFlat()[0],
            repetitionLevels: [0, 1, 0, 1, 0, 1],
            definitionLevels: [3, 3, 3, 2, 3, 3],
            values: [1, 2, 3, 4, 5]
        );

        self::assertSame(3, $data->rowsCount());

        $skipped = $data->skipRows(2);

        self::assertSame(1, $skipped->rowsCount());
        self::assertSame([4, 5], $skipped->values());
        self::assertSame([3, 3], $skipped->definitionLevels());
        self::assertSame([0, 1], $skipped->repetitionLevels());
    }

    public function test_split_by_rows_bug_with_repeated_values_in_same_row() : void
    {
        $schema = Schema::with(
            NestedColumn::list('list', ListElement::int32())
        );

        $data = new WriteFlatColumnValues(
            $schema->columnsFlat()[0],
            repetitionLevels: [0, 1, 1, 0, 1, 0],
            definitionLevels: [3, 3, 3, 3, 3, 3],
            values: [1, 2, 3, 4, 5, 6]
        );

        self::assertSame(3, $data->rowsCount());

        $split = $data->splitByRows(1);

        self::assertCount(3, $split);
        self::assertSame([1, 2, 3], $split[0]->values());
        self::assertSame([4, 5], $split[1]->values());
        self::assertSame([6], $split[2]->values());
    }

    public function test_split_by_rows_with_struct_containing_list_of_strings() : void
    {
        $schema = Schema::with(NestedColumn::struct('struct', [
            NestedColumn::list('list_of_string', ListElement::string()),
        ]));

        $faker = Factory::create();
        $rows = \array_merge(...\array_map(static fn (int $i) : array => [
            [
                'struct' => [
                    'list_of_string' => $i % 2 === 0
                        ? \array_map(
                            static fn ($i) => $faker->text(10),
                            \range(1, 5)
                        )
                        : null,
                ],
            ],
        ], \range(1, 100)));

        $dremel = new DremelShredder(new ColumnDataValidator(), DataConverter::initialize($options = Options::default()));

        $flatColumnData = WriteColumnData::initialize($schema->get('struct'));

        foreach ($rows as $row) {
            foreach ($dremel->shred($schema->get('struct'), $row)->flatValues() as $nextFlatValues) {
                $flatColumnData->addValues($nextFlatValues);
            }
        }

        $flatColumnValues = $flatColumnData->values('struct.list_of_string.list.element');

        $splitResult = $flatColumnValues->splitByRows(20);

        self::assertCount(5, $splitResult, 'Should split into 5 chunks of 20 rows each');
    }

    public function test_splitting_flat_columns_by_rows() : void
    {
        $data = new WriteFlatColumnValues(
            FlatColumn::int32('int32'),
            repetitionLevels: [0, 0, 0, 0, 0, 0, 0],
            definitionLevels: [1, 1, 1, 1, 1, 1, 1],
            values: [1, 2, 3, 4, 5, 6, 7]
        );

        $split = $data->splitByRows(2);

        self::assertCount(4, $split);
        self::assertSame([1, 2], $split[0]->values());
        self::assertSame([3, 4], $split[1]->values());
        self::assertSame([5, 6], $split[2]->values());
        self::assertSame([7], $split[3]->values());
    }
}
