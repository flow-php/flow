<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Bucketing;

use Flow\ETL\Kernels\Bucketing\Buckets;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\BucketMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class BucketsTest extends FlowTestCase
{
    public function test_all_returns_every_registered_bucket(): void
    {
        $buckets = new Buckets(new MemoryRowsStorage());
        $buckets->add(BucketMother::withTotalRows('a', 1));
        $buckets->add(BucketMother::withTotalRows('b', 2));

        static::assertCount(2, $buckets->all());
    }

    public function test_clear_empties_manifest_and_storage(): void
    {
        $storage = new MemoryRowsStorage();
        $storage->append('a', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->append('b', array_to_rows([['id' => 2]], schema(int_schema('id'))));

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
        $storage = new MemoryRowsStorage();
        $storage->append('a', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $buckets = new Buckets($storage);
        $buckets->add(BucketMother::withTotalRows('a', 1));

        $buckets->remove('a');

        static::assertSame([], $buckets->all());
        static::assertSame([], iterator_to_array($storage->get('a'), false));
    }

    public function test_rows_yields_storage_batches(): void
    {
        $storage = new MemoryRowsStorage();
        $storage->append('a', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('a', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        $batches = iterator_to_array((new Buckets($storage))->rows('a'), false);

        static::assertCount(2, $batches);
        static::assertSame([1, 2], $batches[0]->reduceToArray('id'));
        static::assertSame([3], $batches[1]->reduceToArray('id'));
    }

    public function test_rows_of_missing_bucket_yields_nothing(): void
    {
        static::assertSame([], iterator_to_array((new Buckets(new MemoryRowsStorage()))->rows('missing'), false));
    }

    public function test_storage_returns_the_injected_instance(): void
    {
        $storage = new MemoryRowsStorage();

        static::assertSame($storage, (new Buckets($storage))->storage());
    }
}
