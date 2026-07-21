<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing\Storage;

use Flow\ETL\Bucketing\ResidentBucketsStorage;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Row;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class MemoryBucketsTest extends FlowTestCase
{
    public function test_append_accumulates_rows_in_order(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('bucket', rows(row(int_entry('id', 1)), row(int_entry('id', 2))));
        $storage->append('bucket', rows(row(int_entry('id', 3))));

        static::assertSame(
            [1, 2, 3],
            array_map(static fn(Row $r): mixed => $r->valueOf(
                'id',
            ), BucketsStorageContext::rows($storage->get('bucket'))),
        );
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
        $storage->append('bucket', rows(row(int_entry('id', 1))));
        $storage->remove('bucket');

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));
    }

    public function test_set_replaces_previous_rows(): void
    {
        $storage = new MemoryBuckets();
        $storage->append('bucket', rows(row(int_entry('id', 1))));
        $storage->set('bucket', rows(row(int_entry('id', 42))));

        static::assertSame(
            [42],
            array_map(static fn(Row $r): mixed => $r->valueOf(
                'id',
            ), BucketsStorageContext::rows($storage->get('bucket'))),
        );
    }
}
