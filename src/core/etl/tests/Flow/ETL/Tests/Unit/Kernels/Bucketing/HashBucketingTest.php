<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Bucketing;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Kernels\Bucketing\HashBucketing;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\RowsStorageContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Grouping\KeyEncoder;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function hash;
use function hexdec;
use function iterator_to_array;
use function ksort;
use function range;
use function sort;
use function substr;

final class HashBucketingTest extends FlowTestCase
{
    public function test_a_hash_offset_reads_other_digits_of_the_key_hash(): void
    {
        $hash = hash('xxh128', "\0i1");

        static::assertSame(
            (int) hexdec(substr($hash, 0, 8)) % 16,
            (new HashBucketing(new KeyEncoder(['id']), 16, new NativePHPRandomValueGenerator()))->indexOf("\0i1"),
        );
        static::assertSame(
            (int) hexdec(substr($hash, 8, 8)) % 16,
            (new HashBucketing(new KeyEncoder(['id']), 16, new NativePHPRandomValueGenerator(), 'bucket', 8))->indexOf(
                "\0i1",
            ),
        );
    }

    public function test_one_bucket_takes_every_key(): void
    {
        static::assertSame(
            0,
            (new HashBucketing(new KeyEncoder(['id']), 1, new NativePHPRandomValueGenerator()))->indexOf("\0i1"),
        );
    }

    public function test_a_run_id_makes_the_ids_deterministic(): void
    {
        $strategy = new HashBucketing(new KeyEncoder(['id']), 2, new NativePHPRandomValueGenerator(), 'shuffle-1-0');
        $first = [];
        $second = [];

        foreach ([&$first, &$second] as &$ids) {
            $generator = (static function () {
                yield array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
            })();

            foreach ($strategy->bucketize($generator, new MemoryRowsStorage(), new AdaptiveBackend(), '7') as $bucket) {
                $ids[] = $bucket->id;
            }

            sort($ids);
        }

        static::assertSame($first, $second);
        static::assertSame(['shuffle-1-0-7-0', 'shuffle-1-0-7-1'], $first);
    }

    public function test_bucket_ids_carry_namespace_and_run_id(): void
    {
        $strategy = new HashBucketing(new KeyEncoder(['id']), 4, new NativePHPRandomValueGenerator(), 'group-by');

        $generator = (static function () {
            yield array_to_rows([['id' => 1]], schema(int_schema('id')));
        })();

        foreach ($strategy->bucketize($generator, new MemoryRowsStorage(), new AdaptiveBackend()) as $bucket) {
            static::assertMatchesRegularExpression('/^group-by-[0-9a-f]{16}-\d$/', $bucket->id);
        }
    }

    public function test_buckets_are_yielded_after_the_whole_input_is_spilled(): void
    {
        $strategy = new HashBucketing(new KeyEncoder(['id']), 2, new NativePHPRandomValueGenerator());
        $storage = new MemoryRowsStorage();

        $generator = (static function () {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
            yield array_to_rows([['id' => 3], ['id' => 4]], schema(int_schema('id')));
        })();

        $totalRows = 0;

        foreach ($strategy->bucketize($generator, $storage, new AdaptiveBackend()) as $bucket) {
            static::assertCount($bucket->totalRows, RowsStorageContext::rows($storage->get($bucket->id)));
            $totalRows += $bucket->totalRows;
        }

        static::assertSame(4, $totalRows);
    }

    public function test_one_bucket_holds_every_row(): void
    {
        $buckets = iterator_to_array((new HashBucketing(
            new KeyEncoder(['id']),
            1,
            new NativePHPRandomValueGenerator(),
        ))->bucketize(
            (static function () {
                yield array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id')));
            })(),
            new MemoryRowsStorage(),
            new AdaptiveBackend(),
        ));

        static::assertCount(1, $buckets);
        static::assertSame(3, $buckets[0]->totalRows);
    }

    public function test_distributes_rows_within_buckets_count(): void
    {
        $strategy = new HashBucketing(new KeyEncoder(['id']), 4, new NativePHPRandomValueGenerator());

        $rows = [];

        for ($i = 0; $i < 100; $i++) {
            $rows[] = ['id' => $i];
        }

        $generator = (static function () use ($rows) {
            yield array_to_rows($rows, schema(int_schema('id')));
        })();

        $total = 0;

        foreach ($strategy->bucketize($generator, new MemoryRowsStorage(), new AdaptiveBackend()) as $bucket) {
            static::assertMatchesRegularExpression('/-[0-3]$/', $bucket->id);
            $total += $bucket->totalRows;
        }

        static::assertSame(100, $total);
    }

    public function test_index_is_the_partition_number(): void
    {
        $strategy = new HashBucketing(new KeyEncoder(['id']), 4, new NativePHPRandomValueGenerator());

        $rows = [];

        for ($i = 0; $i < 100; $i++) {
            $rows[] = ['id' => $i];
        }

        $generator = (static function () use ($rows) {
            yield array_to_rows($rows, schema(int_schema('id')));
        })();

        $indexes = [];

        foreach ($strategy->bucketize($generator, new MemoryRowsStorage(), new AdaptiveBackend()) as $bucket) {
            static::assertStringEndsWith('-' . $bucket->index, $bucket->id);
            $indexes[] = $bucket->index;
        }

        sort($indexes);
        static::assertSame([0, 1, 2, 3], $indexes);
    }

    public function test_same_key_lands_in_the_same_bucket(): void
    {
        $strategy = new HashBucketing(new KeyEncoder(['id']), 4, new NativePHPRandomValueGenerator());
        $storage = new MemoryRowsStorage();

        $buckets = iterator_to_array($strategy->bucketize(
            (static function () {
                yield array_to_rows([['id' => 1], ['id' => 1]], schema(int_schema('id')));
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
            })(),
            $storage,
            new AdaptiveBackend(),
        ));

        static::assertCount(1, $buckets);
        static::assertSame(3, $buckets[0]->totalRows);
        static::assertCount(3, RowsStorageContext::rows($storage->get($buckets[0]->id)));
    }

    public function test_missing_key_column_is_bucketed_as_null_when_the_schema_allows_it(): void
    {
        $buckets = iterator_to_array((new HashBucketing(
            new KeyEncoder(['id']),
            4,
            new NativePHPRandomValueGenerator(),
        ))->bucketize(
            (static function () {
                yield array_to_rows(
                    [['id' => null], ['other' => 1]],
                    schema(int_schema('id', nullable: true), int_schema('other', nullable: true)),
                );
            })(),
            new MemoryRowsStorage(),
            new AdaptiveBackend(),
        ));

        static::assertCount(1, $buckets);
        static::assertSame(2, $buckets[0]->totalRows);
    }

    public function test_rows_missing_the_key_column_throw(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "id" does not exist.');

        iterator_to_array((new HashBucketing(
            new KeyEncoder(['id']),
            4,
            new NativePHPRandomValueGenerator(),
        ))->bucketize(
            (static function () {
                yield array_to_rows([['other' => 1]], schema(int_schema('other')));
            })(),
            new MemoryRowsStorage(),
            new AdaptiveBackend(),
        ));
    }

    public function test_throws_when_buckets_count_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Buckets count must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new HashBucketing(new KeyEncoder(['id']), 0, new NativePHPRandomValueGenerator());
    }

    public function test_rows_of_one_key_across_batches_stay_in_batch_order(): void
    {
        $strategy = new HashBucketing(new KeyEncoder(['k']), 1, new NativePHPRandomValueGenerator());
        $storage = new MemoryRowsStorage();

        $buckets = iterator_to_array($strategy->bucketize(
            (static function () {
                yield array_to_rows(
                    [['k' => 1, 'v' => 1], ['k' => 2, 'v' => 2], ['k' => 1, 'v' => 3]],
                    schema(int_schema('k'), int_schema('v')),
                );
                yield array_to_rows([['k' => 1, 'v' => 4]], schema(int_schema('k'), int_schema('v')));
            })(),
            $storage,
            new AdaptiveBackend(),
        ));
        $values = [];

        foreach ($storage->get($buckets[0]->id) as $batch) {
            $values = [...$values, ...$batch->column('v')->values()];
        }

        static::assertSame([1, 2, 3, 4], $values);
    }

    public function test_every_row_of_a_unique_key_lands_in_its_key_bucket_in_row_order(): void
    {
        $strategy = new HashBucketing(new KeyEncoder(['id']), 4, new NativePHPRandomValueGenerator());
        $storage = new MemoryRowsStorage();
        $expected = [];

        foreach (range(0, 99) as $id) {
            $expected[$strategy->indexOf("\0i{$id}")][] = $id;
        }

        $actual = [];

        foreach ($strategy->bucketize(
            (static function () {
                yield array_to_rows(
                    array_map(static fn(int $id): array => ['id' => $id], range(0, 99)),
                    schema(int_schema('id')),
                );
            })(),
            $storage,
            new AdaptiveBackend(),
        ) as $bucket) {
            foreach ($storage->get($bucket->id) as $batch) {
                $actual[$bucket->index] = [...($actual[$bucket->index] ?? []), ...$batch->column('id')->values()];
            }
        }

        ksort($expected);
        ksort($actual);
        static::assertSame($expected, $actual);
    }

    public function test_a_batch_without_a_nullable_declared_key_column_lands_with_the_null_keys(): void
    {
        $declared = schema(int_schema('k', nullable: true), int_schema('v'));
        $strategy = new HashBucketing(
            new KeyEncoder(['k'], declared: $declared),
            4,
            new NativePHPRandomValueGenerator(),
        );
        $storage = new MemoryRowsStorage();

        $buckets = iterator_to_array($strategy->bucketize(
            (static function () use ($declared) {
                yield array_to_rows([['k' => null, 'v' => 1]], $declared);
                yield array_to_rows([['v' => 2], ['v' => 3]], schema(int_schema('v')));
            })(),
            $storage,
            new AdaptiveBackend(),
        ));
        $values = [];

        foreach ($storage->get($buckets[0]->id) as $batch) {
            $values = [...$values, ...$batch->column('v')->values()];
        }

        static::assertCount(1, $buckets);
        static::assertSame($strategy->indexOf("\0n"), $buckets[0]->index);
        static::assertSame([1, 2, 3], $values);
    }
}
