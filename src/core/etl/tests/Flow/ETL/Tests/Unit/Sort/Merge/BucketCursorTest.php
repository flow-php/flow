<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort\Merge;

use Flow\ETL\Sort\Merge\BucketCursor;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
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

        $cursor = new BucketCursor($batches());

        $ids = [];

        while ($cursor->valid()) {
            $ids[] = $cursor->current()->get('id');
            $cursor->next();
        }

        static::assertSame([1, 2, 3], $ids);
    }

    public function test_empty_batch_stream_is_invalid_from_the_start(): void
    {
        $batches = static function (): Generator {
            yield from [];
        };

        static::assertFalse((new BucketCursor($batches()))->valid());
    }

    public function test_skips_empty_batches(): void
    {
        $batches = static function (): Generator {
            yield rows(schema());
            yield array_to_rows([['id' => 1]], schema(int_schema('id')));
            yield rows(schema());
            yield array_to_rows([['id' => 2]], schema(int_schema('id')));
        };

        $cursor = new BucketCursor($batches());

        $ids = [];

        while ($cursor->valid()) {
            $ids[] = $cursor->current()->get('id');
            $cursor->next();
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
        $cursor = new BucketCursor($generator);

        static::assertSame(0, $generator->key());

        $cursor->next();

        static::assertSame(0, $generator->key());

        $cursor->next();

        static::assertSame(1, $generator->key());
        static::assertSame(3, $cursor->current()->get('id'));
    }

    public function test_stream_of_only_empty_batches_is_invalid(): void
    {
        $batches = static function (): Generator {
            yield rows(schema());
            yield rows(schema());
        };

        static::assertFalse((new BucketCursor($batches()))->valid());
    }
}
