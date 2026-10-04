<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\JoinException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\HashJoinProcessorMother;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;
use Flow\ETL\Tests\Mother\RowsMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
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
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['id' => 1, 'name' => 'Alice']],
                schema(int_schema('id'), str_schema('name')),
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
        $bigger = array_to_rows(
            [
                ['id' => 1, 'amount' => 100],
                ['id' => 1, 'amount' => 150],
                ['id' => 2, 'amount' => 200],
                ['id' => 3, 'amount' => 300],
            ],
            schema(int_schema('id'), int_schema('amount')),
        );
        $smaller = array_to_rows(
            [['user_id' => 1, 'name' => 'Alice']],
            schema(int_schema('user_id'), str_schema('name')),
        );

        $leftBiggerJoined = [];

        $processor = HashJoinProcessorMother::grace(
            PhysicalPlanMother::reading(from_rows($smaller)),
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
        $leftRows = array_to_rows(
            [['id' => 1, 'amount' => 100], ['id' => 2, 'amount' => 200], ['id' => 404, 'amount' => 300]],
            schema(int_schema('id'), int_schema('amount')),
        );
        $rightRows = array_to_rows(
            [['user_id' => 1, 'name' => 'Alice'], ['user_id' => 2, 'name' => 'Bob'], ['user_id' => 3, 'name' => 'Cid']],
            schema(int_schema('user_id'), str_schema('name')),
        );

        $results = [];

        foreach (['grace', 'resident'] as $mode) {
            $processor = $mode === 'grace'
                ? HashJoinProcessorMother::grace(
                    PhysicalPlanMother::reading(from_rows($rightRows)),
                    Expression::on(['id' => 'user_id']),
                    Join::left,
                )
                : HashJoinProcessorMother::resident(
                    PhysicalPlanMother::reading(from_rows($rightRows)),
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

    public function test_a_build_side_under_the_memory_limit_joins_in_memory_without_bucket_writes(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());
        $processor = HashJoinProcessorMother::with(
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice'], ['user_id' => 2, 'name' => 'Bob']],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::left,
            $storage,
            memoryLimit: Unit::fromGb(64),
        );
        $generator = (static function () {
            yield array_to_rows(
                [['id' => 2, 'amount' => 20], ['id' => 404, 'amount' => 40]],
                schema(int_schema('id'), int_schema('amount')),
            );
        })();

        static::assertSame(
            [
                ['id' => 2, 'amount' => 20, 'user_id' => 2, 'name' => 'Bob'],
                ['id' => 404, 'amount' => 40, 'user_id' => null, 'name' => null],
            ],
            iterator_to_array($processor->process($generator, flow_context()), false)[0]->toArray(),
        );
        static::assertSame([], $storage->appendedRows());
    }

    public function test_a_build_side_past_the_memory_limit_spills_what_it_read_and_the_rest(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());
        $processor = HashJoinProcessorMother::with(
            PhysicalPlanMother::reading(from_rows(
                array_to_rows([['user_id' => 1, 'name' => 'Alice']], schema(int_schema('user_id'), str_schema('name'))),
                array_to_rows([['user_id' => 2, 'name' => 'Bob']], schema(int_schema('user_id'), str_schema('name'))),
            )),
            Expression::on(['id' => 'user_id']),
            Join::inner,
            $storage,
            memoryLimit: Unit::fromBytes(1),
        );
        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        })();

        $joined = [];

        foreach ($processor->process($generator, flow_context()) as $batch) {
            foreach ($batch->toArray() as $row) {
                $joined[] = $row;
            }
        }

        usort($joined, static fn(array $left, array $right): int => (int) $left['id'] <=> (int) $right['id']);

        static::assertSame(
            [['id' => 1, 'user_id' => 1, 'name' => 'Alice'], ['id' => 2, 'user_id' => 2, 'name' => 'Bob']],
            $joined,
        );
        static::assertNotSame([], $storage->appendedRows());
    }

    public function test_handles_empty_left_side(): void
    {
        $processor = HashJoinProcessorMother::grace(
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
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
            PhysicalPlanMother::reading(from_rows(rows(schema()))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1, 'amount' => 100]], schema(int_schema('id'), int_schema('amount')));
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
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice'], ['user_id' => 2, 'name' => 'Bob']],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield array_to_rows(
                [['id' => 1, 'amount' => 100], ['id' => 2, 'amount' => 200], ['id' => 3, 'amount' => 300]],
                schema(int_schema('id'), int_schema('amount')),
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
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
            $storage,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => null], ['id' => null]], schema(int_schema('id', nullable: true)));
        })();

        static::assertCount(0, iterator_to_array($processor->process($generator, flow_context()), false));
        static::assertSame([], $storage->readBucketIds());
    }

    public function test_inner_join_emits_every_matching_right_row(): void
    {
        $processor = HashJoinProcessorMother::grace(
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'role' => 'admin'], ['user_id' => 1, 'role' => 'writer']],
                schema(int_schema('user_id'), str_schema('role')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1]], schema(int_schema('id')));
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
            PhysicalPlanMother::reading(from_rows(array_to_rows([['user_id' => 1]], schema(int_schema('user_id'))))),
            Expression::on(['id' => 'user_id']),
            Join::left_anti,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
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
            PhysicalPlanMother::reading(from_rows(array_to_rows([['user_id' => 1]], schema(int_schema('user_id'))))),
            Expression::on(['id' => 'user_id']),
            Join::left_anti,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => null]], schema(int_schema('id', nullable: true)));
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
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::left,
        );

        $generator = (static function () {
            yield array_to_rows(
                [['id' => 1, 'amount' => 100], ['id' => 2, 'amount' => 200]],
                schema(int_schema('id'), int_schema('amount')),
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
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::left,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => null]], schema(int_schema('id', nullable: true)));
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
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice'], ['user_id' => 2, 'name' => 'Bob']],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(new Any(new Equal('id', 'user_id'))),
            Join::inner,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 3]], schema(int_schema('id')));
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
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [
                    ['user_id' => 1, 'name' => 'Alice'],
                    ['user_id' => 2, 'name' => 'Bob'],
                    ['user_id' => 3, 'name' => 'Cid'],
                ],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 3], ['id' => 1]], schema(int_schema('id')));
            yield array_to_rows([['id' => 2]], schema(int_schema('id')));
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
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice'], ['user_id' => 2, 'name' => 'Bob']],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::right,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1, 'amount' => 100]], schema(int_schema('id'), int_schema('amount')));
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
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice'], ['user_id' => null, 'name' => 'Bob']],
                schema(int_schema('user_id', nullable: true), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::right,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => null]], schema(int_schema('id', nullable: true)));
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
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::left,
            $storage,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        })();

        iterator_to_array($processor->process($generator, flow_context()), false);

        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_pairs_with_a_missing_right_side_read_only_left_buckets(): void
    {
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        // the right side's only key hashes to a bucket no left row lands in, so no pair has a right bucket
        $processor = HashJoinProcessorMother::grace(
            PhysicalPlanMother::reading(from_rows(array_to_rows([['user_id' => 3]], schema(int_schema('user_id'))))),
            Expression::on(['id' => 'user_id']),
            Join::left,
            $storage,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        })();

        iterator_to_array($processor->process($generator, flow_context()), false);

        foreach ($storage->readBucketIds() as $bucketId) {
            static::assertTrue(str_starts_with($bucketId, 'join-left-'), $bucketId);
        }
        static::assertNotSame([], $storage->readBucketIds());
    }

    public function test_resident_join_forwards_stop_to_its_left_upstream(): void
    {
        $upstream = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(5)))->withBatchSize(1);
        $joined = HashJoinProcessorMother::resident(
            PhysicalPlanMother::reading(from_rows(RowsMother::sequentialIds(5))),
            Expression::on(['id' => 'id']),
            Join::inner,
            batchSize: 1,
        )->process($upstream->extract(flow_context()), flow_context());

        static::assertTrue($joined->valid());

        $joined->send(Signal::STOP);

        static::assertFalse($joined->valid());
        static::assertSame(1, $upstream->batchesYielded);
    }

    /**
     * The duplicate is now caught while the joined schema is built, before any row is merged, but
     * HashJoinProcessor still wraps it as a JoinException so the shipped contract holds.
     */
    public function test_duplicated_entries_outside_join_columns_throw_join_exception(): void
    {
        $processor = HashJoinProcessorMother::grace(
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );

        $generator = (static function () {
            yield array_to_rows([['id' => 1, 'name' => 'Norbert']], schema(int_schema('id'), str_schema('name')));
        })();

        $this->expectException(JoinException::class);
        $this->expectExceptionMessage(
            'Entry definitions must be unique, duplicated entries: [name], all: [id, name, user_id, name]',
        );

        iterator_to_array($processor->process($generator, flow_context()), false);
    }

    public function test_the_right_side_is_pulled_through_a_frame_output(): void
    {
        $right = PhysicalPlanMother::reading(from_rows(array_to_rows(
            [['id' => 1, 'name' => 'Alice']],
            schema(int_schema('id'), str_schema('name')),
        )));
        $processor = HashJoinProcessorMother::resident($right, Expression::on(['id' => 'id'], 'r_'), Join::inner);

        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
        })();

        $batches = iterator_to_array($processor->process($generator, flow_context()), false);

        static::assertSame([['id' => 1, 'r_id' => 1, 'r_name' => 'Alice']], $batches[0]->toArray());
    }

    public function test_join_output_gathers_the_input_columns_without_building(): void
    {
        $backend = new SpyBackend();
        $processor = HashJoinProcessorMother::grace(
            PhysicalPlanMother::reading(from_rows(array_to_rows(
                [['user_id' => 1, 'name' => 'Alice']],
                schema(int_schema('user_id'), str_schema('name')),
            ))),
            Expression::on(['id' => 'user_id']),
            Join::inner,
        );
        $generator = (static function () {
            yield array_to_rows([['id' => 1, 'amount' => 100]], schema(int_schema('id'), int_schema('amount')));
        })();

        $joined = iterator_to_array(
            $processor->process($generator, flow_context(config_builder()->backend($backend)->build())),
            false,
        );

        static::assertSame([['id' => 1, 'amount' => 100, 'user_id' => 1, 'name' => 'Alice']], $joined[0]->toArray());
        static::assertSame(0, $backend->builders());
    }
}
