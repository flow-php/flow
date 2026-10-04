<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing\Storage;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Bucketing\Storage\SpillingBuckets;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Config\Bucketing\BucketingConfig;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Context\IdRows;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;

final class SpillingBucketsTest extends FlowTestCase
{
    public function test_under_the_limit_nothing_reaches_the_storage(): void
    {
        $disk = new SpyBucketsStorage(new MemoryBuckets());
        $buckets = new SpillingBuckets($disk, Unit::fromGb(64), new PhpBackend(), 2);

        foreach (IdRows::batches([1], [2, 3], [4]) as $batch) {
            $buckets->append('a', $batch);
        }

        static::assertSame([], $disk->appendedRows());
        static::assertSame([1, 2, 3, 4], IdRows::ids($buckets->get('a')));
    }

    public function test_past_the_limit_every_frame_holds_a_full_batch_and_the_rest_is_read_after_them(): void
    {
        $disk = new SpyBucketsStorage(new MemoryBuckets());
        $buckets = new SpillingBuckets($disk, Unit::fromBytes(1), new PhpBackend(), 2);

        foreach (IdRows::batches([1], [2], [3], [4], [5]) as $batch) {
            $buckets->append('a', $batch);
        }

        static::assertSame(
            ['a' => [2, 2]],
            array_map(static fn(array $frames): array => array_map(
                static fn(Rows $rows): int => $rows->count(),
                $frames,
            ), $disk->appendedRows()),
        );
        static::assertSame([1, 2, 3, 4, 5], IdRows::ids($buckets->get('a')));
    }

    public function test_crossing_the_limit_moves_every_bucket_that_holds_a_full_batch(): void
    {
        $disk = new SpyBucketsStorage(new MemoryBuckets());
        $buckets = new SpillingBuckets($disk, Unit::fromBytes(1), new PhpBackend(), 2);

        $buckets->append('a', IdRows::batches([1, 2, 3])->current());

        static::assertSame([1, 2], IdRows::ids($disk->get('a')));
        static::assertSame([1, 2, 3], IdRows::ids($buckets->get('a')));
    }

    public function test_remove_drops_the_buffer_and_the_frames(): void
    {
        $disk = new SpyBucketsStorage(new MemoryBuckets());
        $buckets = new SpillingBuckets($disk, Unit::fromBytes(1), new PhpBackend(), 1);
        $buckets->append('a', IdRows::batches([1, 2])->current());

        $buckets->remove('a');

        static::assertSame([], IdRows::ids($buckets->get('a')));
        static::assertSame([], $disk->liveBucketIds());
    }

    public function test_set_replaces_what_the_bucket_held(): void
    {
        $buckets = new SpillingBuckets(new MemoryBuckets(), Unit::fromGb(64), new PhpBackend(), 2);
        $buckets->append('a', IdRows::batches([1])->current());

        $buckets->set('a', IdRows::batches([7, 8])->current());

        static::assertSame([7, 8], IdRows::ids($buckets->get('a')));
    }

    public function test_an_unknown_bucket_reads_nothing(): void
    {
        static::assertSame(
            [],
            IdRows::ids((new SpillingBuckets(new MemoryBuckets(), Unit::fromGb(64), new PhpBackend()))->get('missing')),
        );
    }

    public function test_around_keeps_a_resident_storage_and_wraps_any_other(): void
    {
        $resident = new MemoryBuckets();
        $disk = new SpyBucketsStorage(new MemoryBuckets());

        static::assertSame($resident, SpillingBuckets::around(
            new BucketingConfig($resident, 64, 1000),
            Unit::fromMb(1),
            new PhpBackend(),
        ));

        $wrapped = SpillingBuckets::around(new BucketingConfig($disk, 64, 500), Unit::fromMb(3), new PhpBackend());

        static::assertInstanceOf(SpillingBuckets::class, $wrapped);
        static::assertSame($disk, $wrapped->disk);
        static::assertSame(Unit::fromMb(3)->inBytes(), $wrapped->memoryLimit->inBytes());
    }
}
