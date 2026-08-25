<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\BucketRun;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\BucketMother;

use function array_map;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class BucketRunTest extends FlowTestCase
{
    public function test_remove_removes_through_its_own_buckets(): void
    {
        $a = new Buckets($storageA = new MemoryBuckets());
        $b = new Buckets($storageB = new MemoryBuckets());

        $storageA->append('shared-id', rows(row(int_entry('id', 1))));
        $a->add(BucketMother::withTotalRows('shared-id', 1));
        $storageB->append('shared-id', rows(row(int_entry('id', 2))));
        $b->add(BucketMother::withTotalRows('shared-id', 1));

        (new BucketRun('shared-id', $a))->remove();

        static::assertSame([], BucketsStorageContext::rows($storageA->get('shared-id')));
        static::assertCount(1, BucketsStorageContext::rows($storageB->get('shared-id')));
    }

    public function test_rows_are_read_through_the_buckets_that_wrote_them(): void
    {
        $a = new Buckets($storageA = new MemoryBuckets());
        $b = new Buckets($storageB = new MemoryBuckets());

        $storageA->append('shared-id', rows(row(int_entry('id', 1))));
        $storageB->append('shared-id', rows(row(int_entry('id', 2))));

        static::assertSame(
            [1],
            array_map(static fn($r): mixed => $r->valueOf(
                'id',
            ), BucketsStorageContext::rows((new BucketRun('shared-id', $a))->rows())),
        );
        static::assertSame(
            [2],
            array_map(static fn($r): mixed => $r->valueOf(
                'id',
            ), BucketsStorageContext::rows((new BucketRun('shared-id', $b))->rows())),
        );
    }
}
