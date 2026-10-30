<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Storage\PsrRowsStorage;
use Flow\ETL\Tests\Context\RowsStorageContext;
use Flow\ETL\Tests\Double\ArrayCache;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\AdaptiveBackend;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PsrRowsStorageTest extends FlowTestCase
{
    public function test_append_then_get_returns_rows_in_order_across_chunks(): void
    {
        $storage = new PsrRowsStorage(new ArrayCache(), new FloeSerializer(new AdaptiveBackend()));
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        static::assertSame([1, 2, 3], array_column(RowsStorageContext::rows($storage->get('bucket')), 'id'));
    }

    public function test_custom_prefix_is_used_in_keys(): void
    {
        $cache = new ArrayCache();
        (new PsrRowsStorage(
            $cache,
            new FloeSerializer(new AdaptiveBackend()),
            prefix: 'custom.pfx',
        ))->append('bucket', array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))));

        static::assertTrue($cache->has('custom.pfx.bucket.entry'));
        static::assertTrue($cache->has('custom.pfx.bucket.chunk.0'));
    }

    public function test_get_skips_non_string_chunk_payloads(): void
    {
        $cache = new ArrayCache();
        $cache->set('flow.rows.bucket.entry', '{"chunks":1,"rows":1,"schema":[]}');
        $cache->set('flow.rows.bucket.chunk.0', 123);

        static::assertSame(
            [],
            RowsStorageContext::rows((new PsrRowsStorage($cache, new FloeSerializer(new AdaptiveBackend())))->get(
                'bucket',
            )),
        );
    }

    public function test_get_treats_a_malformed_entry_as_empty(): void
    {
        $cache = new ArrayCache();
        $cache->set('flow.rows.bucket.entry', 'garbage');

        static::assertSame(
            [],
            RowsStorageContext::rows((new PsrRowsStorage($cache, new FloeSerializer(new AdaptiveBackend())))->get(
                'bucket',
            )),
        );
    }

    public function test_get_unknown_bucket_yields_nothing(): void
    {
        static::assertSame(
            [],
            RowsStorageContext::rows((new PsrRowsStorage(
                new ArrayCache(),
                new FloeSerializer(new AdaptiveBackend()),
            ))->get('unknown')),
        );
    }

    public function test_remove_deletes_all_chunk_keys_and_the_entry(): void
    {
        $cache = new ArrayCache();
        $storage = new PsrRowsStorage($cache, new FloeSerializer(new AdaptiveBackend()));
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 2]], schema(int_schema('id'))));

        $storage->remove('bucket');

        static::assertFalse($cache->has('flow.rows.bucket.entry'));
        static::assertFalse($cache->has('flow.rows.bucket.chunk.0'));
        static::assertFalse($cache->has('flow.rows.bucket.chunk.1'));
        static::assertSame([], RowsStorageContext::rows($storage->get('bucket')));
    }

    public function test_round_trips_schema_changing_rows(): void
    {
        $storage = new PsrRowsStorage(new ArrayCache(), new FloeSerializer(new AdaptiveBackend()));
        $input = array_to_rows(
            [['id' => 1], ['id' => 2, 'name' => 'John'], ['name' => 'Jane']],
            schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
        );
        $storage->append('bucket', $input);

        static::assertSame($input->toArray(), RowsStorageContext::rows($storage->get('bucket')));
    }

    #[TestWith(['flow{buckets'])]
    #[TestWith(['flow}buckets'])]
    #[TestWith(['flow(buckets'])]
    #[TestWith(['flow)buckets'])]
    #[TestWith(['flow/buckets'])]
    #[TestWith(['flow\\buckets'])]
    #[TestWith(['flow@buckets'])]
    #[TestWith(['flow:buckets'])]
    public function test_rejects_prefix_with_psr16_reserved_character(string $prefix): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Prefix must not contain PSR-16 reserved characters {}()/\\@:, given: '
        . $prefix);

        new PsrRowsStorage(new ArrayCache(), new FloeSerializer(new AdaptiveBackend()), prefix: $prefix);
    }

    public function test_has_rows_and_schema_describe_the_appended_batches(): void
    {
        $storage = new PsrRowsStorage(new ArrayCache(), new FloeSerializer(new AdaptiveBackend()));
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => null]], schema(int_schema('id', nullable: true))));

        static::assertTrue($storage->has('bucket'));
        static::assertSame(3, $storage->rows('bucket')->exactly());
        static::assertEquals(schema(int_schema('id', nullable: true)), $storage->schema('bucket'));
    }

    public function test_a_missing_bucket_has_no_rows_and_no_schema(): void
    {
        $storage = new PsrRowsStorage(new ArrayCache(), new FloeSerializer(new AdaptiveBackend()));

        static::assertFalse($storage->has('missing'));
        static::assertSame(0, $storage->rows('missing')->exactly());
        static::assertNull($storage->schema('missing'));
    }
}
