<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Bucket;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketShape;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\SortedRunBucketing;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function count;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function iterator_to_array;

final class BucketingProcessorTest extends FlowTestCase
{
    public function test_empty_input_registers_no_buckets_and_yields_nothing(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $processor = new BucketingProcessor(
            new HashBucketing([ref('id')], 2, new NativeHasher(), new NativePHPRandomValueGenerator()),
            $buckets,
        );

        $generator = (static function () {
            yield from [];
        })();

        static::assertCount(0, iterator_to_array($processor->process($generator, flow_context(config()))));
        static::assertCount(0, $buckets->all());
    }

    public function test_registers_every_yielded_bucket_in_the_repository(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $processor = new BucketingProcessor(
            new HashBucketing([ref('id')], 2, new NativeHasher(), new NativePHPRandomValueGenerator()),
            $buckets,
        );

        $generator = (static function () {
            yield rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 4)),
            );
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context(config())));

        static::assertCount(count($buckets->all()), $result);

        $registeredIds = array_map(static fn(Bucket $bucket): string => $bucket->id, $buckets->all());

        foreach ($result as $batch) {
            static::assertContains($batch->first()->valueOf(BucketShape::id->value), $registeredIds);
        }
    }

    public function test_yields_one_metadata_rows_batch_per_bucket(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $processor = new BucketingProcessor(
            new SortedRunBucketing([ref('id')], 2, new NativePHPRandomValueGenerator()),
            $buckets,
        );

        $generator = (static function () {
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context(config())));

        static::assertCount(2, $result);

        $totalRows = 0;

        foreach ($result as $batch) {
            static::assertCount(1, $batch);
            $totalRows += (int) $batch->first()->valueOf(BucketShape::totalRows->value);
        }

        static::assertSame(3, $totalRows);
    }
}
