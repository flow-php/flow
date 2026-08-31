<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\Double\ConstantHasher;
use Flow\ETL\Tests\Double\CountingHasher;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;
use function sort;

final class HashBucketingTest extends FlowTestCase
{
    public function test_bucket_ids_carry_namespace_and_run_id(): void
    {
        $strategy = new HashBucketing(
            [ref('id')],
            4,
            new NativeHasher(),
            new NativePHPRandomValueGenerator(),
            'group-by',
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]));
        })();

        foreach ($strategy->bucketize($generator, new MemoryBuckets()) as $bucket) {
            static::assertMatchesRegularExpression('/^group-by-[0-9a-f]{16}-\d$/', $bucket->id);
        }
    }

    public function test_buckets_are_yielded_after_the_whole_input_is_spilled(): void
    {
        $strategy = new HashBucketing([ref('id')], 2, new NativeHasher(), new NativePHPRandomValueGenerator());
        $storage = new MemoryBuckets();

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
            yield rows(schema(int_schema('id')), row(['id' => 3]), row(['id' => 4]));
        })();

        $totalRows = 0;

        foreach ($strategy->bucketize($generator, $storage) as $bucket) {
            static::assertCount($bucket->totalRows, BucketsStorageContext::rows($storage->get($bucket->id)));
            $totalRows += $bucket->totalRows;
        }

        static::assertSame(4, $totalRows);
    }

    public function test_custom_hasher_overrides_assignment(): void
    {
        $strategy = new HashBucketing(
            [ref('id')],
            8,
            new ConstantHasher('00000000ffffffff'),
            new NativePHPRandomValueGenerator(),
        );

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3]));
        })();

        $buckets = iterator_to_array($strategy->bucketize($generator, new MemoryBuckets()));

        static::assertCount(1, $buckets);
        static::assertSame(3, $buckets[0]->totalRows);
    }

    public function test_distributes_rows_within_buckets_count(): void
    {
        $strategy = new HashBucketing([ref('id')], 4, new NativeHasher(), new NativePHPRandomValueGenerator());

        $rows = [];

        for ($i = 0; $i < 100; $i++) {
            $rows[] = row(['id' => $i]);
        }

        $generator = (static function () use ($rows) {
            yield rows(schema(int_schema('id')), ...$rows);
        })();

        $total = 0;

        foreach ($strategy->bucketize($generator, new MemoryBuckets()) as $bucket) {
            static::assertMatchesRegularExpression('/-[0-3]$/', $bucket->id);
            $total += $bucket->totalRows;
        }

        static::assertSame(100, $total);
    }

    public function test_index_is_the_partition_number(): void
    {
        $strategy = new HashBucketing([ref('id')], 4, new NativeHasher(), new NativePHPRandomValueGenerator());

        $rows = [];

        for ($i = 0; $i < 100; $i++) {
            $rows[] = row(['id' => $i]);
        }

        $generator = (static function () use ($rows) {
            yield rows(schema(int_schema('id')), ...$rows);
        })();

        $indexes = [];

        foreach ($strategy->bucketize($generator, new MemoryBuckets()) as $bucket) {
            static::assertStringEndsWith('-' . $bucket->index, $bucket->id);
            $indexes[] = $bucket->index;
        }

        sort($indexes);
        static::assertSame([0, 1, 2, 3], $indexes);
    }

    public function test_hashes_each_row_exactly_once(): void
    {
        $spy = new CountingHasher(new NativeHasher());
        $strategy = new HashBucketing([ref('id')], 4, $spy, new NativePHPRandomValueGenerator());

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]));
            yield rows(schema(int_schema('id')), row(['id' => 3]));
        })();

        iterator_to_array($strategy->bucketize($generator, new MemoryBuckets()));

        static::assertSame(3, $spy->hashedRows());
    }

    public function test_same_key_lands_in_the_same_bucket(): void
    {
        $strategy = new HashBucketing([ref('id')], 4, new NativeHasher(), new NativePHPRandomValueGenerator());
        $storage = new MemoryBuckets();

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 1]));
            yield rows(schema(int_schema('id')), row(['id' => 1]));
        })();

        $buckets = iterator_to_array($strategy->bucketize($generator, $storage));

        static::assertCount(1, $buckets);
        static::assertSame(3, $buckets[0]->totalRows);
        static::assertCount(3, BucketsStorageContext::rows($storage->get($buckets[0]->id)));
    }

    public function test_missing_key_column_is_bucketed_as_null_when_the_schema_allows_it(): void
    {
        $strategy = new HashBucketing([ref('id')], 4, new NativeHasher(), new NativePHPRandomValueGenerator());
        $storage = new MemoryBuckets();

        $generator = (static function () {
            yield rows(
                schema(int_schema('id', nullable: true), int_schema('other', nullable: true)),
                row(['id' => null]),
                row(['other' => 1]),
            );
        })();

        $buckets = iterator_to_array($strategy->bucketize($generator, $storage));

        static::assertCount(1, $buckets);
        static::assertSame(2, $buckets[0]->totalRows);
    }

    public function test_rows_missing_the_key_column_throw_by_default(): void
    {
        $strategy = new HashBucketing([ref('id')], 4, new NativeHasher(), new NativePHPRandomValueGenerator());

        $generator = (static function () {
            yield rows(schema(int_schema('other')), row(['other' => 1]));
        })();

        $this->expectException(InvalidArgumentException::class);

        iterator_to_array($strategy->bucketize($generator, new MemoryBuckets()));
    }

    public function test_throws_when_buckets_count_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Buckets count must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new HashBucketing([ref('id')], 0, new NativeHasher(), new NativePHPRandomValueGenerator());
    }
}
