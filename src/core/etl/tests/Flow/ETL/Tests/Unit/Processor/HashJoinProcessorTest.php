<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\JoinException;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\HashJoinProcessorMother;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;
use function str_starts_with;
use function usort;

final class HashJoinProcessorTest extends FlowTestCase
{
    public function test_bind_derives_the_joined_schema_from_both_sides(): void
    {
        $processor = HashJoinProcessorMother::resident(
            df()->read(from_rows(rows(
                schema(int_schema('id'), str_schema('name')),
                row(['id' => 1, 'name' => 'Alice']),
            ))),
            Expression::on(['id' => 'id']),
            Join::inner,
        );

        static::assertEquals(
            schema(int_schema('id'), int_schema('amount'), str_schema('name')),
            $processor->bind(schema(int_schema('id'), int_schema('amount')))->output,
        );
    }

    public function test_builds_hash_table_from_the_smaller_side_without_changing_results(): void
    {
        $bigger = rows(
            schema(int_schema('id'), int_schema('amount')),
            row(['id' => 1, 'amount' => 100]),
            row(['id' => 1, 'amount' => 150]),
            row(['id' => 2, 'amount' => 200]),
            row(['id' => 3, 'amount' => 300]),
        );
        $smaller = rows(schema(int_schema('user_id'), str_schema('name')), row(['user_id' => 1, 'name' => 'Alice']));

        $leftBiggerJoined = [];

        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows($smaller)),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () use ($bigger) {
            yield $bigger;
        })();

        $batches = iterator_to_array($processor->process($generator, flow_context()), false);

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $leftBiggerJoined[] = $rowData;
            }
        }

        usort(
            $leftBiggerJoined,
            static fn(array $left, array $right): int => (int) $left['amount'] <=> (int) $right['amount'],
        );

        static::assertSame(
            [
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'name' => 'Alice'],
                ['id' => 1, 'amount' => 150, 'user_id' => 1, 'name' => 'Alice'],
            ],
            $leftBiggerJoined,
        );
    }

    public function test_grace_and_resident_storages_produce_the_same_rows(): void
    {
        $leftRows = rows(
            schema(int_schema('id'), int_schema('amount')),
            row(['id' => 1, 'amount' => 100]),
            row(['id' => 2, 'amount' => 200]),
            row(['id' => 404, 'amount' => 300]),
        );
        $rightRows = rows(
            schema(int_schema('user_id'), str_schema('name')),
            row(['user_id' => 1, 'name' => 'Alice']),
            row(['user_id' => 2, 'name' => 'Bob']),
            row(['user_id' => 3, 'name' => 'Cid']),
        );

        $results = [];

        foreach (['grace', 'resident'] as $mode) {
            $processor = $mode === 'grace'
                ? HashJoinProcessorMother::grace(
                    df()->read(from_rows($rightRows)),
                    Expression::on(['id' => 'user_id']),
                    Join::left,
                )
                : HashJoinProcessorMother::resident(
                    df()->read(from_rows($rightRows)),
                    Expression::on(['id' => 'user_id']),
                    Join::left,
                );

            $generator = (static function () use ($leftRows) {
                yield $leftRows;
            })();

            $joined = [];

            $batches = iterator_to_array($processor->process($generator, flow_context()), false);

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
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield from [];
        })();

        static::assertCount(0, iterator_to_array($processor->process($generator, flow_context()), false));
    }

    public function test_handles_empty_right_side(): void
    {
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(schema()))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id'), int_schema('amount')), row(['id' => 1, 'amount' => 100]));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()), false);
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
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
                row(['user_id' => 2, 'name' => 'Bob']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
                row(['id' => 2, 'amount' => 200]),
                row(['id' => 3, 'amount' => 300]),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()), false);
        /** @var list<array<array-key, mixed>> $allRows */
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        usort($allRows, static fn(array $left, array $right): int => (int) $left['id'] <=> (int) $right['id']);

        static::assertCount(2, $allRows);
        static::assertEquals(1, $allRows[0]['id']);
        static::assertEquals('Alice', $allRows[0]['name']);
        static::assertEquals(2, $allRows[1]['id']);
        static::assertEquals('Bob', $allRows[1]['name']);
    }

    public function test_inner_join_drops_null_key_rows_without_touching_storage(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
            $storage,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id', nullable: true)), row(['id' => null]), row(['id' => null]));
        })();

        static::assertCount(0, iterator_to_array($processor->process($generator, flow_context()), false));
        static::assertSame([], $storage->readBucketIds());
    }

    public function test_inner_join_emits_every_matching_right_row(): void
    {
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('role')),
                row(['user_id' => 1, 'role' => 'admin']),
                row(['user_id' => 1, 'role' => 'writer']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]));
        })();

        $joined = [];

        $batches = iterator_to_array($processor->process($generator, flow_context()), false);

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
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(schema(int_schema('user_id')), row(['user_id' => 1])))),
            Expression::on(['id' => 'user_id']),
            Join::left_anti,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        })();

        $joined = [];

        $batches = iterator_to_array($processor->process($generator, flow_context()), false);

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => 2]], $joined);
    }

    public function test_left_anti_join_keeps_null_key_left_rows(): void
    {
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(schema(int_schema('user_id')), row(['user_id' => 1])))),
            Expression::on(['id' => 'user_id']),
            Join::left_anti,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id', nullable: true)), row(['id' => 1]), row(['id' => null]));
        })();

        $joined = [];

        $batches = iterator_to_array($processor->process($generator, flow_context()), false);

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => null]], $joined);
    }

    public function test_left_join(): void
    {
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::left,
        );

        $generator = (static function () {
            yield rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => 1, 'amount' => 100]),
                row(['id' => 2, 'amount' => 200]),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()), false);
        /** @var list<array<array-key, mixed>> $allRows */
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        usort($allRows, static fn(array $left, array $right): int => (int) $left['id'] <=> (int) $right['id']);

        static::assertCount(2, $allRows);
        static::assertEquals('Alice', $allRows[0]['name']);
        static::assertNull($allRows[1]['name']);
    }

    public function test_left_join_pads_null_key_left_rows(): void
    {
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::left,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id', nullable: true)), row(['id' => 1]), row(['id' => null]));
        })();

        $joined = [];

        $batches = iterator_to_array($processor->process($generator, flow_context()), false);

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        usort($joined, static fn(array $left, array $right): int => (int) $left['id'] <=> (int) $right['id']);

        static::assertSame(
            [
                ['id' => null, 'user_id' => null, 'name' => null],
                ['id' => 1, 'user_id' => 1, 'name' => 'Alice'],
            ],
            $joined,
        );
    }

    public function test_non_equality_join_falls_back_to_a_single_bucket(): void
    {
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
                row(['user_id' => 2, 'name' => 'Bob']),
            ))),
            Expression::on(new Any(new Equal('id', 'user_id'))),
            Join::inner,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 3]));
        })();

        $joined = [];

        $batches = iterator_to_array($processor->process($generator, flow_context()), false);

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame([['id' => 1, 'user_id' => 1, 'name' => 'Alice']], $joined);
    }

    public function test_resident_storage_preserves_left_row_order(): void
    {
        $processor = HashJoinProcessorMother::resident(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
                row(['user_id' => 2, 'name' => 'Bob']),
                row(['user_id' => 3, 'name' => 'Cid']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 3]), row(['id' => 1]));
            yield rows(schema(int_schema('id')), row(['id' => 2]));
        })();

        $joined = [];

        $batches = iterator_to_array($processor->process($generator, flow_context()), false);

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        static::assertSame(
            [
                ['id' => 3, 'user_id' => 3, 'name' => 'Cid'],
                ['id' => 1, 'user_id' => 1, 'name' => 'Alice'],
                ['id' => 2, 'user_id' => 2, 'name' => 'Bob'],
            ],
            $joined,
        );
    }

    public function test_right_join(): void
    {
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
                row(['user_id' => 2, 'name' => 'Bob']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::right,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id'), int_schema('amount')), row(['id' => 1, 'amount' => 100]));
        })();

        $joined = [];

        $batches = iterator_to_array($processor->process($generator, flow_context()), false);

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        usort($joined, static fn(array $left, array $right): int => (int) $left['user_id'] <=> (int) $right['user_id']);

        static::assertSame(
            [
                ['id' => 1, 'amount' => 100, 'user_id' => 1, 'name' => 'Alice'],
                ['id' => null, 'amount' => null, 'user_id' => 2, 'name' => 'Bob'],
            ],
            $joined,
        );
    }

    public function test_right_join_pads_null_key_right_rows(): void
    {
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id', nullable: true), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
                row(['user_id' => null, 'name' => 'Bob']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::right,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id', nullable: true)), row(['id' => 1]), row(['id' => null]));
        })();

        $joined = [];

        $batches = iterator_to_array($processor->process($generator, flow_context()), false);

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $joined[] = $rowData;
            }
        }

        usort($joined, static fn(array $left, array $right): int => (int) $left['user_id'] <=> (int) $right['user_id']);

        static::assertSame(
            [
                ['id' => null, 'user_id' => null, 'name' => 'Bob'],
                ['id' => 1, 'user_id' => 1, 'name' => 'Alice'],
            ],
            $joined,
        );
    }

    public function test_storages_are_cleared_after_the_join(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::left,
            $storage,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        })();

        iterator_to_array($processor->process($generator, flow_context()), false);

        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_pairs_with_a_missing_right_side_read_only_left_buckets(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(schema()))),
            Expression::on(['id' => 'user_id']),
            Join::left,
            $storage,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
        })();

        iterator_to_array($processor->process($generator, flow_context()), false);

        foreach ($storage->readBucketIds() as $bucketId) {
            static::assertTrue(str_starts_with($bucketId, 'join-left-'), $bucketId);
        }
        static::assertNotSame([], $storage->readBucketIds());
    }

    /**
     * The duplicate is now caught while the joined schema is built, before any row is merged, but
     * HashJoinProcessor still wraps it as a JoinException so the shipped contract holds.
     */
    public function test_duplicated_entries_outside_join_columns_throw_join_exception(): void
    {
        $processor = HashJoinProcessorMother::grace(
            df()->read(from_rows(rows(
                schema(int_schema('user_id'), str_schema('name')),
                row(['user_id' => 1, 'name' => 'Alice']),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id'), str_schema('name')), row(['id' => 1, 'name' => 'Norbert']));
        })();

        $this->expectException(JoinException::class);
        $this->expectExceptionMessage(
            'Entry definitions must be unique, duplicated entries: [name], all: [id, name, user_id, name]',
        );

        iterator_to_array($processor->process($generator, flow_context()), false);
    }
}
