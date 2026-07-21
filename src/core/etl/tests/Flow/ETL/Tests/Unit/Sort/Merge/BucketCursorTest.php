<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort\Merge;

use Flow\ETL\Rows;
use Flow\ETL\Sort\Merge\BucketCursor;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class BucketCursorTest extends FlowTestCase
{
    public function test_iterates_rows_across_batch_boundaries(): void
    {
        $batches = static function (): Generator {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)));
        };

        $cursor = new BucketCursor($batches());

        $ids = [];

        while ($cursor->valid()) {
            $ids[] = $cursor->current()->valueOf('id');
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
            yield new Rows();
            yield rows(row(int_entry('id', 1)));
            yield new Rows();
            yield rows(row(int_entry('id', 2)));
        };

        $cursor = new BucketCursor($batches());

        $ids = [];

        while ($cursor->valid()) {
            $ids[] = $cursor->current()->valueOf('id');
            $cursor->next();
        }

        static::assertSame([1, 2], $ids);
    }

    public function test_next_batch_is_not_decoded_until_the_current_one_is_exhausted(): void
    {
        $batches = static function (): Generator {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)));
        };

        $generator = $batches();
        $cursor = new BucketCursor($generator);

        static::assertSame(0, $generator->key());

        $cursor->next();

        static::assertSame(0, $generator->key());

        $cursor->next();

        static::assertSame(1, $generator->key());
        static::assertSame(3, $cursor->current()->valueOf('id'));
    }

    public function test_stream_of_only_empty_batches_is_invalid(): void
    {
        $batches = static function (): Generator {
            yield new Rows();
            yield new Rows();
        };

        static::assertFalse((new BucketCursor($batches()))->valid());
    }
}
