<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage;

use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\ResidentRowsStorage;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\RowsStorageContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class MemoryRowsStorageTest extends FlowTestCase
{
    public function test_describe_round_trips_to_an_empty_storage(): void
    {
        $storage = new MemoryRowsStorage();
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $rebuilt = DescribeRoundTrip::rebuilt($storage);

        static::assertSame([], $storage->describe()?->data);
        static::assertCount(0, RowsStorageContext::rows($rebuilt->get('bucket')));
    }

    public function test_append_accumulates_rows_in_order(): void
    {
        $storage = new MemoryRowsStorage();
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        static::assertSame([1, 2, 3], array_column(RowsStorageContext::rows($storage->get('bucket')), 'id'));
    }

    public function test_get_unknown_bucket_yields_nothing(): void
    {
        static::assertSame([], RowsStorageContext::rows((new MemoryRowsStorage())->get('unknown')));
    }

    public function test_is_resident_storage(): void
    {
        static::assertInstanceOf(ResidentRowsStorage::class, new MemoryRowsStorage());
    }

    public function test_remove_drops_the_bucket(): void
    {
        $storage = new MemoryRowsStorage();
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->remove('bucket');

        static::assertSame([], RowsStorageContext::rows($storage->get('bucket')));
    }

    public function test_an_empty_append_makes_the_bucket_present_with_no_rows(): void
    {
        $storage = new MemoryRowsStorage();
        $storage->append('bucket', Rows::empty(new Schema(), new PhpBackend()));

        static::assertTrue($storage->has('bucket'));
        static::assertSame(0, $storage->rows('bucket')->exactly());
        static::assertEquals(new Schema(), $storage->schema('bucket'));
    }

    public function test_has_rows_and_schema_describe_the_appended_batches(): void
    {
        $storage = new MemoryRowsStorage();
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        static::assertTrue($storage->has('bucket'));
        static::assertSame(3, $storage->rows('bucket')->exactly());
        static::assertEquals(schema(int_schema('id')), $storage->schema('bucket'));
    }

    public function test_a_missing_bucket_has_no_rows_and_no_schema(): void
    {
        $storage = new MemoryRowsStorage();

        static::assertFalse($storage->has('missing'));
        static::assertSame(0, $storage->rows('missing')->exactly());
        static::assertNull($storage->schema('missing'));
    }
}
