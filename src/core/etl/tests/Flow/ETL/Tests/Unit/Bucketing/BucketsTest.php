<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\BucketMother;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function iterator_to_array;

final class BucketsTest extends FlowTestCase
{
    public function test_all_returns_every_registered_bucket(): void
    {
        $buckets = new Buckets(new MemoryBuckets());
        $buckets->add(BucketMother::withTotalRows('a', 1));
        $buckets->add(BucketMother::withTotalRows('b', 2));

        static::assertCount(2, $buckets->all());
    }

    public function test_clear_empties_manifest_and_storage(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('a', rows(row(int_entry('id', 1))));
        $storage->append('b', rows(row(int_entry('id', 2))));

        $buckets = new Buckets($storage);
        $buckets->add(BucketMother::withTotalRows('a', 1));
        $buckets->add(BucketMother::withTotalRows('b', 1));

        $buckets->clear();

        static::assertSame([], $buckets->all());
        static::assertSame([], iterator_to_array($storage->get('a'), false));
        static::assertSame([], iterator_to_array($storage->get('b'), false));
    }

    public function test_remove_drops_manifest_entry_and_storage(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('a', rows(row(int_entry('id', 1))));

        $buckets = new Buckets($storage);
        $buckets->add(BucketMother::withTotalRows('a', 1));

        $buckets->remove('a');

        static::assertSame([], $buckets->all());
        static::assertSame([], iterator_to_array($storage->get('a'), false));
    }

    public function test_rows_yields_storage_batches(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('a', rows(row(int_entry('id', 1)), row(int_entry('id', 2))));
        $storage->append('a', rows(row(int_entry('id', 3))));

        $batches = iterator_to_array((new Buckets($storage))->rows('a'), false);

        static::assertCount(2, $batches);
        static::assertSame([1, 2], $batches[0]->reduceToArray('id'));
        static::assertSame([3], $batches[1]->reduceToArray('id'));
    }

    public function test_rows_of_missing_bucket_yields_nothing(): void
    {
        static::assertSame([], iterator_to_array((new Buckets(new MemoryBuckets()))->rows('missing'), false));
    }

    public function test_storage_returns_the_injected_instance(): void
    {
        $storage = new MemoryBuckets();

        static::assertSame($storage, (new Buckets($storage))->storage());
    }
}
