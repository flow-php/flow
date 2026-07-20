<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\BucketYield;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\SortedRunBucketing;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CountingHasher;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class BucketingProcessorTest extends FlowTestCase
{
    public function test_clear_removes_spilled_data_from_storage(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $processor = new BucketingProcessor(new HashBucketing(refs('id'), 2), $buckets);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        iterator_to_array($processor->process($generator, flow_context(config())));

        $ids = [];

        foreach ($buckets->all() as $bucket) {
            $ids[] = $bucket->id;
        }

        $buckets->clear();

        static::assertCount(0, $buckets->all());

        foreach ($ids as $id) {
            static::assertCount(0, iterator_to_array($buckets->storage()->get($id)));
        }
    }

    public function test_empty_input_registers_no_buckets_and_yields_nothing(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $processor = new BucketingProcessor(new HashBucketing(refs('id'), 2), $buckets);

        $generator = (static function () {
            yield from [];
        })();

        static::assertCount(0, iterator_to_array($processor->process($generator, flow_context(config()))));
        static::assertCount(0, $buckets->all());
    }

    public function test_hashes_each_row_exactly_once_end_to_end(): void
    {
        $spy = new CountingHasher(new NativeHasher());
        $buckets = new Buckets(new MemoryBuckets());
        $processor = new BucketingProcessor(new HashBucketing(refs('id'), 2, $spy), $buckets);

        $generator = (static function () {
            yield rows(
                row(int_entry('id', 1)),
                row(int_entry('id', 2)),
                row(int_entry('id', 3)),
                row(int_entry('id', 4)),
            );
        })();

        iterator_to_array($processor->process($generator, flow_context(config())));

        static::assertSame(4, $spy->hashedRows());
    }

    public function test_none_yield_spills_and_registers_but_yields_nothing(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $processor = new BucketingProcessor(new SortedRunBucketing(refs('id'), 2), $buckets, BucketYield::none);

        $generator = (static function () {
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        static::assertCount(0, iterator_to_array($processor->process($generator, flow_context(config()))));
        static::assertNotEmpty($buckets->all());

        foreach ($buckets->all() as $bucket) {
            static::assertEquals(refs('id'), $bucket->stats->sortedBy());
        }
    }

    public function test_rows_yield_attaches_bucket_entry_and_registers_stats(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $processor = new BucketingProcessor(new HashBucketing(refs('id'), 2), $buckets);

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

        $bucketIds = [];
        $totalStatsRows = 0;

        foreach ($buckets->all() as $bucket) {
            $bucketIds[] = $bucket->id;
            $totalStatsRows += $bucket->stats->rowsCount();
        }

        static::assertSame(4, $totalStatsRows);

        $yielded = 0;

        foreach ($result as $batch) {
            foreach ($batch as $row) {
                $yielded++;
                static::assertTrue($row->has(BucketingProcessor::BUCKET_ENTRY));
                static::assertContains($row->valueOf(BucketingProcessor::BUCKET_ENTRY), $bucketIds);
            }
        }

        static::assertSame(4, $yielded);
    }

    public function test_rows_yield_is_homogeneous_per_batch(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $processor = new BucketingProcessor(new HashBucketing(refs('id'), 2), $buckets, BucketYield::rows, 2);

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

        foreach ($result as $batch) {
            $ids = [];

            foreach ($batch as $row) {
                $value = $row->valueOf(BucketingProcessor::BUCKET_ENTRY);
                static::assertIsString($value);
                $ids[$value] = true;
            }

            static::assertCount(1, $ids);
        }
    }

    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        new BucketingProcessor(
            new HashBucketing(refs('id'), 2),
            new Buckets(new MemoryBuckets()),
            BucketYield::rows,
            0,
        );
    }
}
