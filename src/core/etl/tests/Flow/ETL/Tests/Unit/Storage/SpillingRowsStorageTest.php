<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Storage;

use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Storage\SpillingRowsStorage;
use Flow\ETL\Tests\Context\IdRows;
use Flow\ETL\Tests\Double\SpyRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class SpillingRowsStorageTest extends FlowTestCase
{
    public function test_under_the_limit_nothing_reaches_the_storage(): void
    {
        $disk = new SpyRowsStorage(new MemoryRowsStorage());
        $buckets = new SpillingRowsStorage($disk, Unit::fromGb(64), new PhpBackend(), new BatchSize(2));

        foreach (IdRows::batches([1], [2, 3], [4]) as $batch) {
            $buckets->append('a', $batch);
        }

        static::assertSame([], $disk->appendedRows());
        static::assertSame([1, 2, 3, 4], IdRows::ids($buckets->get('a')));
    }

    public function test_past_the_limit_every_frame_holds_a_full_batch_and_the_rest_is_read_after_them(): void
    {
        $disk = new SpyRowsStorage(new MemoryRowsStorage());
        $buckets = new SpillingRowsStorage($disk, Unit::fromBytes(1), new PhpBackend(), new BatchSize(2));

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
        $disk = new SpyRowsStorage(new MemoryRowsStorage());
        $buckets = new SpillingRowsStorage($disk, Unit::fromBytes(1), new PhpBackend(), new BatchSize(2));

        $buckets->append('a', IdRows::batches([1, 2, 3])->current());

        static::assertSame([1, 2], IdRows::ids($disk->get('a')));
        static::assertSame([1, 2, 3], IdRows::ids($buckets->get('a')));
    }

    public function test_remove_drops_the_buffer_and_the_frames(): void
    {
        $disk = new SpyRowsStorage(new MemoryRowsStorage());
        $buckets = new SpillingRowsStorage($disk, Unit::fromBytes(1), new PhpBackend(), new BatchSize(1));
        $buckets->append('a', IdRows::batches([1, 2])->current());

        $buckets->remove('a');

        static::assertSame([], IdRows::ids($buckets->get('a')));
        static::assertSame([], $disk->liveBucketIds());
    }

    public function test_has_rows_and_schema_count_the_frames_and_the_buffer(): void
    {
        $buckets = new SpillingRowsStorage(
            new MemoryRowsStorage(),
            Unit::fromBytes(1),
            new PhpBackend(),
            new BatchSize(2),
        );
        $buckets->append('a', IdRows::batches([1, 2, 3])->current());

        static::assertTrue($buckets->has('a'));
        static::assertSame(3, $buckets->rows('a')->exactly());
        static::assertEquals(schema(int_schema('id')), $buckets->schema('a'));
    }

    public function test_an_empty_append_makes_the_bucket_present_with_no_rows(): void
    {
        $buckets = new SpillingRowsStorage(new MemoryRowsStorage(), Unit::fromGb(64), new PhpBackend());
        $buckets->append('a', Rows::empty(new Schema(), new PhpBackend()));

        static::assertTrue($buckets->has('a'));
        static::assertSame(0, $buckets->rows('a')->exactly());
        static::assertEquals(new Schema(), $buckets->schema('a'));
        static::assertSame([], IdRows::ids($buckets->get('a')));
    }

    public function test_an_unknown_bucket_reads_nothing(): void
    {
        static::assertSame(
            [],
            IdRows::ids((new SpillingRowsStorage(new MemoryRowsStorage(), Unit::fromGb(64), new PhpBackend()))->get(
                'missing',
            )),
        );
    }
}
