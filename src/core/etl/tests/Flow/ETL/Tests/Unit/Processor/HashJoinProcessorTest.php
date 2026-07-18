<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Sort\ExternalSort\BucketsCache\InMemoryBucketsCache;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function usort;

final class HashJoinProcessorTest extends FlowTestCase
{
    public function test_on_disk_cache_produces_the_same_rows_as_the_in_memory_cache(): void
    {
        $leftRows = rows(
            row(int_entry('id', 1), int_entry('amount', 100)),
            row(int_entry('id', 2), int_entry('amount', 200)),
            row(int_entry('id', 404), int_entry('amount', 300)),
        );
        $rightRows = rows(
            row(int_entry('user_id', 1), str_entry('name', 'Alice')),
            row(int_entry('user_id', 2), str_entry('name', 'Bob')),
            row(int_entry('user_id', 3), str_entry('name', 'Cid')),
        );

        $results = [];

        foreach ([config_builder()->joinCache(new InMemoryBucketsCache()), config_builder()] as $configBuilder) {
            $processor = new HashJoinProcessor(
                df()->read(from_rows($rightRows)),
                Expression::on(['id' => 'user_id']),
                Join::left,
            );

            $generator = (static function () use ($leftRows) {
                yield $leftRows;
            })();

            $joined = [];

            $context = flow_context($configBuilder->build());

            /** @var list<Rows> $batches */
            $batches = iterator_to_array($processor->process($generator, $context));

            foreach ($batches as $batch) {
                foreach ($batch->toArray() as $rowData) {
                    $joined[] = $rowData;
                }
            }

            usort($joined, static fn(array $left, array $right): int => (int) $left['id'] <=> (int) $right['id']);
            $results[] = $joined;
        }

        static::assertSame(
            [
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'amount' => 200, 'user_id' => 2, 'name' => 'Bob'],
                ['id' => 404, 'amount' => 300, 'user_id' => null, 'name' => null],
            ],
            $results[0],
        );
        static::assertSame($results[0], $results[1]);
    }

    public function test_handles_empty_left_side(): void
    {
        $rightDf = df()->read(from_rows(rows(row(int_entry('user_id', 1), str_entry('name', 'Alice')))));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::inner);

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(0, $result);
    }

    public function test_handles_empty_right_side(): void
    {
        $rightDf = df()->read(from_rows(rows()));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::inner);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1), int_entry('amount', 100)));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(0, $allRows);
    }

    public function test_inner_join(): void
    {
        $rightDf = df()->read(from_rows(rows(
            row(int_entry('user_id', 1), str_entry('name', 'Alice')),
            row(int_entry('user_id', 2), str_entry('name', 'Bob')),
        )));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::inner);

        $generator = (static function () {
            yield rows(
                row(int_entry('id', 1), int_entry('amount', 100)),
                row(int_entry('id', 2), int_entry('amount', 200)),
                row(int_entry('id', 3), int_entry('amount', 300)),
            );
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        /** @var list<array<array-key, mixed>> $allRows */
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(2, $allRows);
        static::assertEquals(1, $allRows[0]['id']);
        static::assertEquals('Alice', $allRows[0]['name']);
        static::assertEquals(2, $allRows[1]['id']);
        static::assertEquals('Bob', $allRows[1]['name']);
    }

    public function test_inner_join_emits_every_matching_right_row(): void
    {
        $rightDf = df()->read(from_rows(rows(
            row(int_entry('user_id', 1), str_entry('role', 'admin')),
            row(int_entry('user_id', 1), str_entry('role', 'writer')),
        )));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::inner);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)));
        })();

        $joined = [];

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($processor->process($generator, flow_context()));

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'user_id' => 1, 'role' => 'admin'],
                ['id' => 1, 'user_id' => 1, 'role' => 'writer'],
            ],
            $joined,
        );
    }

    public function test_left_anti_join(): void
    {
        $rightDf = df()->read(from_rows(rows(row(int_entry('user_id', 1)))));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::left_anti);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        $joined = [];

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($processor->process($generator, flow_context()));

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => 2]], $joined);
    }

    public function test_left_join(): void
    {
        $rightDf = df()->read(from_rows(rows(row(int_entry('user_id', 1), str_entry('name', 'Alice')))));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::left);

        $generator = (static function () {
            yield rows(
                row(int_entry('id', 1), int_entry('amount', 100)),
                row(int_entry('id', 2), int_entry('amount', 200)),
            );
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        /** @var list<array<array-key, mixed>> $allRows */
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(2, $allRows);
        static::assertEquals('Alice', $allRows[0]['name']);
        static::assertNull($allRows[1]['name']);
    }

    public function test_right_join(): void
    {
        $rightDf = df()->read(from_rows(rows(
            row(int_entry('user_id', 1), str_entry('name', 'Alice')),
            row(int_entry('user_id', 2), str_entry('name', 'Bob')),
        )));

        $processor = new HashJoinProcessor($rightDf, Expression::on(['id' => 'user_id']), Join::right);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1), int_entry('amount', 100)));
        })();

        $joined = [];

        /** @var list<Rows> $batches */
        $batches = iterator_to_array($processor->process($generator, flow_context()));

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'name' => 'Alice'],
                ['id' => null, 'amount' => null, 'user_id' => 2, 'name' => 'Bob'],
            ],
            $joined,
        );
    }
}
