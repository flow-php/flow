<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing\Storage;

use Flow\ETL\Bucketing\ResidentBucketsStorage;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class MemoryBucketsTest extends FlowTestCase
{
    public function test_append_accumulates_rows_in_order(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        static::assertSame([1, 2, 3], array_column(BucketsStorageContext::rows($storage->get('bucket')), 'id'));
    }

    public function test_get_unknown_bucket_yields_nothing(): void
    {
        static::assertSame([], BucketsStorageContext::rows((new MemoryBuckets())->get('unknown')));
    }

    public function test_is_resident_storage(): void
    {
        static::assertInstanceOf(ResidentBucketsStorage::class, new MemoryBuckets());
    }

    public function test_remove_drops_the_bucket(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->remove('bucket');

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));
    }

    public function test_set_replaces_previous_rows(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->set('bucket', array_to_rows([['id' => 42]], schema(int_schema('id'))));

        static::assertSame([42], array_column(BucketsStorageContext::rows($storage->get('bucket')), 'id'));
    }
}
