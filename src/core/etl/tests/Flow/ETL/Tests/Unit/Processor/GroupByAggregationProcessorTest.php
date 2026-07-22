<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;

final class GroupByAggregationProcessorTest extends FlowTestCase
{
    public function test_aggregates_every_bucket_from_the_metadata_stream(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $buckets = new Buckets(new MemoryBuckets());
        $strategy = new HashBucketing(
            [ref('category')],
            4,
            new NativeHasher(),
            new NativePHPRandomValueGenerator(),
            'group-by',
        );

        $input = (static function (): Generator {
            yield rows(
                row(str_entry('category', 'a'), int_entry('amount', 10)),
                row(str_entry('category', 'b'), int_entry('amount', 15)),
                row(str_entry('category', 'a'), int_entry('amount', 20)),
            );
        })();

        $metadata = [];

        foreach ($strategy->bucketize($input, $buckets->storage()) as $bucket) {
            $buckets->add($bucket);
            $metadata[] = rows($bucket->toRow());
        }

        $result = iterator_to_array(
            (new GroupByAggregationProcessor($groupBy, $buckets))->process(
                (static function () use ($metadata): Generator {
                    yield from $metadata;
                })(),
                flow_context(),
            ),
            preserve_keys: false,
        );

        $aggregated = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $groupRow) {
                $aggregated[$groupRow['category']] = $groupRow['amount_sum'];
            }
        }

        ksort($aggregated);

        static::assertSame(['a' => 30, 'b' => 15], $aggregated);
    }

    public function test_aggregates_every_bucket_when_metadata_rows_arrive_in_one_batch(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $buckets = new Buckets(new MemoryBuckets());
        $strategy = new HashBucketing(
            [ref('category')],
            4,
            new NativeHasher(),
            new NativePHPRandomValueGenerator(),
            'group-by',
        );

        $input = (static function (): Generator {
            yield rows(
                row(str_entry('category', 'a'), int_entry('amount', 10)),
                row(str_entry('category', 'b'), int_entry('amount', 15)),
                row(str_entry('category', 'a'), int_entry('amount', 20)),
            );
        })();

        $metadataRows = [];

        foreach ($strategy->bucketize($input, $buckets->storage()) as $bucket) {
            $buckets->add($bucket);
            $metadataRows[] = $bucket->toRow();
        }

        $result = iterator_to_array(
            (new GroupByAggregationProcessor($groupBy, $buckets))->process(
                (static function () use ($metadataRows): Generator {
                    yield rows(...$metadataRows);
                })(),
                flow_context(),
            ),
            preserve_keys: false,
        );

        $aggregated = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $groupRow) {
                $aggregated[$groupRow['category']] = $groupRow['amount_sum'];
            }
        }

        ksort($aggregated);

        static::assertSame(['a' => 30, 'b' => 15], $aggregated);
    }

    public function test_clears_storage_after_the_last_bucket(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $storage = new SpyBucketsStorage(new MemoryBuckets());
        $buckets = new Buckets($storage);
        $strategy = new HashBucketing(
            [ref('category')],
            4,
            new NativeHasher(),
            new NativePHPRandomValueGenerator(),
            'group-by',
        );

        $input = (static function (): Generator {
            yield rows(
                row(str_entry('category', 'a'), int_entry('amount', 10)),
                row(str_entry('category', 'b'), int_entry('amount', 15)),
            );
        })();

        $metadata = [];

        foreach ($strategy->bucketize($input, $buckets->storage()) as $bucket) {
            $buckets->add($bucket);
            $metadata[] = rows($bucket->toRow());
        }

        iterator_to_array(
            (new GroupByAggregationProcessor($groupBy, $buckets))->process(
                (static function () use ($metadata): Generator {
                    yield from $metadata;
                })(),
                flow_context(),
            ),
            preserve_keys: false,
        );

        static::assertNotSame([], $storage->readBucketIds());
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        new GroupByAggregationProcessor(new GroupBy(ref('category')), new Buckets(new MemoryBuckets()), 0);
    }

    public function test_handles_an_empty_metadata_stream(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $result = iterator_to_array(
            (new GroupByAggregationProcessor($groupBy, new Buckets(new MemoryBuckets())))->process(
                (static function (): Generator {
                    yield from [];
                })(),
                flow_context(),
            ),
            preserve_keys: false,
        );

        static::assertSame([], $result);
    }
}
