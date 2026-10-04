<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort\Merge;

use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Sort\Merge\BucketCursor;
use Flow\ETL\Sort\RowOrder;
use Flow\ETL\Sort\SortKey;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class BucketCursorTest extends FlowTestCase
{
    public function test_iterates_rows_across_batch_boundaries(): void
    {
        $batches = static function (): Generator {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
            yield array_to_rows([['id' => 3]], schema(int_schema('id')));
        };

        $cursor = new BucketCursor($batches(), new RowOrder([ref('id')]), new AdaptiveBackend());

        $ids = [];

        while ($cursor->valid()) {
            $ids[] = $cursor->batch()->column('id')->value($cursor->index());
            $cursor->advance($cursor->index() + 1);
        }

        static::assertSame([1, 2, 3], $ids);
    }

    public function test_empty_batch_stream_is_invalid_from_the_start(): void
    {
        $batches = static function (): Generator {
            yield from [];
        };

        static::assertFalse((new BucketCursor($batches(), new RowOrder([ref('id')]), new AdaptiveBackend()))->valid());
    }

    public function test_skips_empty_batches(): void
    {
        $batches = static function (): Generator {
            yield rows(schema());
            yield array_to_rows([['id' => 1]], schema(int_schema('id')));
            yield rows(schema());
            yield array_to_rows([['id' => 2]], schema(int_schema('id')));
        };

        $cursor = new BucketCursor($batches(), new RowOrder([ref('id')]), new AdaptiveBackend());

        $ids = [];

        while ($cursor->valid()) {
            $ids[] = $cursor->batch()->column('id')->value($cursor->index());
            $cursor->advance($cursor->index() + 1);
        }

        static::assertSame([1, 2], $ids);
    }

    public function test_next_batch_is_not_decoded_until_the_current_one_is_exhausted(): void
    {
        $batches = static function (): Generator {
            yield array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));
            yield array_to_rows([['id' => 3]], schema(int_schema('id')));
        };

        $generator = $batches();
        $cursor = new BucketCursor($generator, new RowOrder([ref('id')]), new AdaptiveBackend());

        static::assertSame(0, $generator->key());

        $cursor->advance(1);

        static::assertSame(0, $generator->key());

        $cursor->advance(2);

        static::assertSame(1, $generator->key());
        static::assertSame(0, $cursor->index());
        static::assertSame([['id' => 3]], $cursor->batch()->toArray());
    }

    public function test_stream_of_only_empty_batches_is_invalid(): void
    {
        $batches = static function (): Generator {
            yield rows(schema());
            yield rows(schema());
        };

        static::assertFalse((new BucketCursor($batches(), new RowOrder([ref('id')]), new AdaptiveBackend()))->valid());
    }

    public function test_keys_are_the_sort_keys_of_every_ref_in_order(): void
    {
        $batches = static function (): Generator {
            yield array_to_rows(
                [['id' => 2, 'at' => new DateTimeImmutable('1970-01-01 00:00:01', new DateTimeZone('UTC'))]],
                schema(int_schema('id'), datetime_schema('at')),
            );
        };

        $cursor = new BucketCursor($batches(), new RowOrder([ref('at'), ref('id')]), new AdaptiveBackend());

        static::assertSame(
            [[1_000_000], [2]],
            array_map(static fn(SortKey $key): array => $key->values, $cursor->keys()),
        );
        static::assertEquals(schema(int_schema('id'), datetime_schema('at')), $cursor->schema());
    }

    public function test_schema_of_an_empty_stream_is_empty(): void
    {
        $batches = static function (): Generator {
            yield from [];
        };

        static::assertEquals(
            schema(),
            (new BucketCursor($batches(), new RowOrder([ref('id')]), new AdaptiveBackend()))->schema(),
        );
    }
}
