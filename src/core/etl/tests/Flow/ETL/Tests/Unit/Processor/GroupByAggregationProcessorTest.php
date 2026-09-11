<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\GroupBy;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
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
                schema(str_schema('category'), int_schema('amount')),
                row(['category' => 'a', 'amount' => 10]),
                row(['category' => 'b', 'amount' => 15]),
                row(['category' => 'a', 'amount' => 20]),
            );
        })();

        $metadata = [];

        foreach ($strategy->bucketize($input, $buckets->storage()) as $bucket) {
            $buckets->add($bucket);
            $metadata[] = rows(Bucket::schema(), $bucket->toRow());
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

        static::assertSame(['a' => 30.0, 'b' => 15.0], $aggregated);
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
                schema(str_schema('category'), int_schema('amount')),
                row(['category' => 'a', 'amount' => 10]),
                row(['category' => 'b', 'amount' => 15]),
                row(['category' => 'a', 'amount' => 20]),
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
                    yield rows(Bucket::schema(), ...$metadataRows);
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

        static::assertSame(['a' => 30.0, 'b' => 15.0], $aggregated);
    }

    public function test_bind_derives_the_group_by_columns_followed_by_the_aggregations(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        static::assertEquals(
            schema(str_schema('category'), float_schema('amount_sum', nullable: true)),
            (new GroupByAggregationProcessor($groupBy, new Buckets(new MemoryBuckets())))->bind(schema(
                str_schema('category'),
                int_schema('amount'),
                str_schema('dropped'),
            ))->output,
        );
    }

    public function test_bind_refuses_a_group_by_column_missing_from_the_input(): void
    {
        $groupBy = new GroupBy(ref('missing'));
        $groupBy->aggregate(sum(ref('amount')));

        $this->expectException(SchemaDefinitionNotFoundException::class);

        (new GroupByAggregationProcessor($groupBy, new Buckets(new MemoryBuckets())))->bind(schema(int_schema(
            'amount',
        )));
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
                schema(str_schema('category'), int_schema('amount')),
                row(['category' => 'a', 'amount' => 10]),
                row(['category' => 'b', 'amount' => 15]),
            );
        })();

        $metadata = [];

        foreach ($strategy->bucketize($input, $buckets->storage()) as $bucket) {
            $buckets->add($bucket);
            $metadata[] = rows(Bucket::schema(), $bucket->toRow());
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

    public function test_a_bound_global_aggregate_over_an_empty_stream_emits_one_row_of_defaults(): void
    {
        $groupBy = new GroupBy();
        $groupBy->aggregate(sum(ref('amount')), count(ref('amount')));

        $bound = (new GroupByAggregationProcessor($groupBy, new Buckets(new MemoryBuckets())))->bind(schema(int_schema(
            'amount',
        )));

        static::assertInstanceOf(GroupByAggregationProcessor::class, $bound->step);

        $result = iterator_to_array(
            $bound->step->process(
                (static function (): Generator {
                    yield from [];
                })(),
                flow_context(),
            ),
            preserve_keys: false,
        );

        static::assertCount(1, $result);
        static::assertSame([['amount_sum' => null, 'amount_count' => 0]], $result[0]->toArray());
        static::assertEquals($bound->output, $result[0]->schema());
    }

    public function test_a_bound_keyed_aggregate_over_an_empty_stream_emits_nothing(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $bound = (new GroupByAggregationProcessor($groupBy, new Buckets(new MemoryBuckets())))->bind(schema(
            str_schema('category'),
            int_schema('amount'),
        ));

        static::assertInstanceOf(GroupByAggregationProcessor::class, $bound->step);

        static::assertSame(
            [],
            iterator_to_array(
                $bound->step->process(
                    (static function (): Generator {
                        yield from [];
                    })(),
                    flow_context(),
                ),
                preserve_keys: false,
            ),
        );
    }

    public function test_an_unbound_global_aggregate_over_an_empty_stream_emits_nothing(): void
    {
        $groupBy = new GroupBy();
        $groupBy->aggregate(sum(ref('amount')), count(ref('amount')));

        static::assertSame(
            [],
            iterator_to_array(
                (new GroupByAggregationProcessor($groupBy, new Buckets(new MemoryBuckets())))->process(
                    (static function (): Generator {
                        yield from [];
                    })(),
                    flow_context(),
                ),
                preserve_keys: false,
            ),
        );
    }

    public function test_a_bound_global_aggregate_with_input_emits_no_extra_row(): void
    {
        $groupBy = new GroupBy();
        $groupBy->aggregate(sum(ref('amount')), count(ref('amount')));

        $buckets = new Buckets(new MemoryBuckets());
        $metadata = GroupByContext::bucketMetadata(
            $buckets,
            [],
            rows(schema(int_schema('amount')), row(['amount' => 10]), row(['amount' => 20])),
        );

        $bound = (new GroupByAggregationProcessor($groupBy, $buckets))->bind(schema(int_schema('amount')));

        static::assertInstanceOf(GroupByAggregationProcessor::class, $bound->step);

        $result = iterator_to_array(
            $bound->step->process(
                (static function () use ($metadata): Generator {
                    yield from $metadata;
                })(),
                flow_context(),
            ),
            preserve_keys: false,
        );

        static::assertCount(1, $result);
        static::assertSame([['amount_sum' => 30.0, 'amount_count' => 2]], $result[0]->toArray());
    }
}
