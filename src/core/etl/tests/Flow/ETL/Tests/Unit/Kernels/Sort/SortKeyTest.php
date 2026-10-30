<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Sort;

use Flow\ETL\Kernels\Sort\SortKey;
use Flow\ETL\Tests\FlowTestCase;

use const SORT_NUMERIC;
use const SORT_STRING;

final class SortKeyTest extends FlowTestCase
{
    public function test_concat_joins_the_keys_under_the_first_flag_and_direction(): void
    {
        $key = SortKey::concat(
            new SortKey([1, 0], [3, 0], SORT_NUMERIC, true),
            new SortKey([1], [7], SORT_STRING, false),
            new SortKey([2, 1], [0, 9], SORT_NUMERIC, false),
        );

        static::assertSame([1, 0, 1, 2, 1], $key->ranks);
        static::assertSame([3, 0, 7, 0, 9], $key->values);
        static::assertSame(SORT_NUMERIC, $key->flag);
        static::assertTrue($key->descending);
        static::assertSame([1], SortKey::concat(new SortKey([1], ['a'], SORT_STRING, false))->ranks);
    }

    public function test_slice_keeps_the_rows_of_the_window_reindexed(): void
    {
        $slice = (new SortKey([1, 1, 0, 2], ['a', 'b', '', 0], SORT_STRING, false))->slice(1, 2);

        static::assertSame([1, 0], $slice->ranks);
        static::assertSame(['b', ''], $slice->values);
        static::assertSame(SORT_STRING, $slice->flag);
        static::assertFalse($slice->descending);
        static::assertSame([], (new SortKey([1], [1], SORT_NUMERIC, false))->slice(1, 5)->values);
    }
}
