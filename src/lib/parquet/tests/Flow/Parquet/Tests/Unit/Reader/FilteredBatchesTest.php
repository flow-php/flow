<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Filter\Binding\BoundFilter;
use Flow\Parquet\Filter\Evaluation\FilterEvaluator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader\FilteredBatches;
use Generator;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\Parquet\DSL\parquet_column;
use function iterator_to_array;

final class FilteredBatchesTest extends TestCase
{
    public function test_the_batches_hold_the_returned_columns_and_the_filters_root_columns(): void
    {
        $batches = new FilteredBatches(
            new FilterEvaluator(BoundFilter::bind(
                parquet_column('address.city')->equals('x'),
                Schema::with(FlatColumn::int64('id'), NestedColumn::struct('address', [FlatColumn::string('city')])),
                new Options(),
            )),
            ['id'],
        );

        static::assertSame(['id', 'address'], $batches->columns());
    }

    public function test_only_the_matching_rows_of_the_returned_columns_are_kept(): void
    {
        $batches = new FilteredBatches(
            new FilterEvaluator(BoundFilter::bind(
                parquet_column('id')->in([2, 4]),
                Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')),
                new Options(),
            )),
            ['name'],
        );

        static::assertSame(
            [['name' => ['b', 'd']]],
            iterator_to_array($batches->filter([
                ['id' => [1, 2, 3], 'name' => ['a', 'b', 'c']],
                ['id' => [4], 'name' => ['d']],
            ]), false),
        );
    }

    public function test_without_an_evaluator_every_row_is_kept_and_only_the_offset_and_the_limit_apply(): void
    {
        $batches = new FilteredBatches(null, ['id'], 1, 2);

        static::assertSame(['id'], $batches->columns());
        static::assertSame(
            [['id' => [2, 3]]],
            iterator_to_array($batches->filter([['id' => [1, 2]], ['id' => [3, 4]]]), false),
        );
    }

    public function test_offset_and_limit_count_the_matching_rows_across_batches_and_no_batch_is_pulled_past_the_limit(): void
    {
        $pulled = 0;
        $source = (
            /** @return Generator<int, array<string, list<mixed>>> */
            static function () use (&$pulled): Generator {
                foreach ([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10]] as $ids) {
                    $pulled++;

                    yield ['id' => $ids, 'name' => array_map('strval', $ids)];
                }
            }
        )();

        $batches = new FilteredBatches(
            new FilterEvaluator(BoundFilter::bind(
                parquet_column('id')->greaterThan(1),
                Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')),
                new Options(),
            )),
            ['id'],
            offset: 2,
            limit: 3,
        );

        static::assertSame([['id' => [4, 5, 6]]], iterator_to_array($batches->filter($source), false));
        static::assertSame(2, $pulled);
    }

    public function test_the_matching_rows_are_rechunked_to_the_batch_size(): void
    {
        $batches = new FilteredBatches(
            new FilterEvaluator(BoundFilter::bind(
                parquet_column('id')->greaterThan(1),
                Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')),
                new Options(),
            )),
            ['id'],
            batchSize: 2,
        );

        static::assertSame(
            [['id' => [2, 3]], ['id' => [4, 5]], ['id' => [6, 7]], ['id' => [8]]],
            iterator_to_array($batches->filter([
                ['id' => [1, 2, 3, 4, 5], 'name' => ['', '', '', '', '']],
                ['id' => [6, 7, 8], 'name' => ['', '', '']],
            ]), false),
        );
    }

    public function test_a_limit_of_zero_pulls_no_batch(): void
    {
        $pulled = 0;
        $source = (
            /** @return Generator<int, array<string, list<mixed>>> */
            static function () use (&$pulled): Generator {
                $pulled++;

                yield ['id' => [2], 'name' => ['']];
            }
        )();

        $batches = new FilteredBatches(
            new FilterEvaluator(BoundFilter::bind(
                parquet_column('id')->greaterThan(1),
                Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')),
                new Options(),
            )),
            ['id'],
            limit: 0,
        );

        static::assertSame([], iterator_to_array($batches->filter($source)));
        static::assertSame(0, $pulled);
    }

    public function test_no_returned_column_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A filtered read returns at least one column');

        new FilteredBatches(
            new FilterEvaluator(BoundFilter::bind(
                parquet_column('id')->isNull(),
                Schema::with(FlatColumn::int64('id'), FlatColumn::string('name')),
                new Options(),
            )),
            [],
        );
    }
}
