<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\HashJoin;

use Flow\ETL\Join\HashJoin\BucketSpiller;
use Flow\ETL\Tests\Double\MemoryBucketsCache;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function iterator_to_array;

final class BucketSpillerTest extends FlowTestCase
{
    public function test_buckets_written_only_when_rows_were_added(): void
    {
        $spiller = new BucketSpiller(new MemoryBucketsCache(), 'join-run-right-bucket-', 10);

        $spiller->add(3, row(int_entry('id', 1)));
        $spiller->flush();

        static::assertSame([3 => 'join-run-right-bucket-3'], $spiller->bucketIds());
    }

    public function test_flush_writes_remaining_rows(): void
    {
        $cache = new MemoryBucketsCache();
        $spiller = new BucketSpiller($cache, 'join-run-left-bucket-', 10);

        $spiller->add(0, row(int_entry('id', 1)));
        $spiller->add(0, row(int_entry('id', 2)));
        $spiller->add(1, row(int_entry('id', 3)));

        static::assertSame([], $spiller->bucketIds());

        $spiller->flush();

        static::assertCount(2, iterator_to_array($cache->get('join-run-left-bucket-0'), false));
        static::assertCount(1, iterator_to_array($cache->get('join-run-left-bucket-1'), false));
    }

    public function test_total_buffer_cap_flushes_all_buckets(): void
    {
        $cache = new MemoryBucketsCache();
        $spiller = new BucketSpiller($cache, 'join-run-left-bucket-', 4);

        $spiller->add(0, row(int_entry('id', 1)));
        $spiller->add(1, row(int_entry('id', 2)));
        $spiller->add(0, row(int_entry('id', 3)));
        $spiller->add(2, row(int_entry('id', 4)));

        static::assertSame(1, $cache->appendCalls['join-run-left-bucket-0']);
        static::assertSame(1, $cache->appendCalls['join-run-left-bucket-1']);
        static::assertSame(1, $cache->appendCalls['join-run-left-bucket-2']);
        static::assertCount(2, iterator_to_array($cache->get('join-run-left-bucket-0'), false));
    }

    public function test_rows_are_appended_in_batches(): void
    {
        $cache = new MemoryBucketsCache();
        $spiller = new BucketSpiller($cache, 'join-run-left-bucket-', 2);

        $spiller->add(0, row(int_entry('id', 1)));
        $spiller->add(0, row(int_entry('id', 2)));
        $spiller->add(0, row(int_entry('id', 3)));
        $spiller->add(0, row(int_entry('id', 4)));
        $spiller->add(0, row(int_entry('id', 5)));
        $spiller->flush();

        static::assertSame(3, $cache->appendCalls['join-run-left-bucket-0']);
        static::assertCount(5, iterator_to_array($cache->get('join-run-left-bucket-0'), false));
    }
}
