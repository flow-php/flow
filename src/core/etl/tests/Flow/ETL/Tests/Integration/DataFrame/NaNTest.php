<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;

use const NAN;

final class NaNTest extends FlowIntegrationTestCase
{
    public function test_nan_is_greater_than_every_number(): void
    {
        static::assertSame(
            ['nan', 'five'],
            df()
                ->read(from_array([
                    ['id' => 'nan', 'f' => NAN],
                    ['id' => 'five', 'f' => 5.0],
                    ['id' => 'one', 'f' => 1.0],
                ]))
                ->filter(ref('f')->greaterThan(lit(2.0)))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_nan_is_never_less_than_a_number(): void
    {
        static::assertSame(
            ['one'],
            df()
                ->read(from_array([
                    ['id' => 'nan', 'f' => NAN],
                    ['id' => 'five', 'f' => 5.0],
                    ['id' => 'one', 'f' => 1.0],
                ]))
                ->filter(ref('f')->lessThan(lit(2.0)))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_nan_lies_above_every_closed_range(): void
    {
        static::assertSame(
            ['five'],
            df()
                ->read(from_array([['id' => 'nan', 'f' => NAN], ['id' => 'five', 'f' => 5.0]]))
                ->filter(ref('f')->between(lit(0.0), lit(10.0)))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_nan_equals_nan(): void
    {
        static::assertSame(
            ['first', 'second'],
            df()
                ->read(from_array([
                    ['id' => 'first', 'f' => NAN],
                    ['id' => 'second', 'f' => NAN],
                    ['id' => 'one', 'f' => 1.0],
                ]))
                ->filter(ref('f')->equals(lit(NAN)))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_nan_sorts_after_every_number(): void
    {
        static::assertSame(
            ['one', 'five', 'nan'],
            df()
                ->read(from_array([
                    ['id' => 'nan', 'f' => NAN],
                    ['id' => 'five', 'f' => 5.0],
                    ['id' => 'one', 'f' => 1.0],
                ]))
                ->sortBy(ref('f'))
                ->fetch()
                ->reduceToArray('id'),
        );
    }

    public function test_nans_form_one_group(): void
    {
        $groups = df()
            ->read(from_array([['f' => NAN, 'i' => 1], ['f' => NAN, 'i' => 2]]))
            ->groupBy(ref('f'))
            ->aggregate(sum(ref('i')))
            ->fetch()
            ->toArray();

        static::assertCount(1, $groups);
        static::assertNan($groups[0]['f']);
        static::assertSame(3.0, $groups[0]['i_sum']);
    }

    public function test_nans_form_one_batch(): void
    {
        $sizes = [];

        df()
            ->read(from_array([['f' => NAN, 'i' => 1], ['f' => NAN, 'i' => 2], ['f' => 1.0, 'i' => 3]]))
            ->batchBy('f')
            ->forEach(static function ($rows) use (&$sizes): void {
                $sizes[] = $rows->count();
            });

        static::assertSame([2, 1], $sizes);
    }

    public function test_nans_are_one_distinct_value(): void
    {
        static::assertSame(
            [1, 3],
            df()
                ->read(from_array([['f' => NAN, 'i' => 1], ['f' => NAN, 'i' => 2], ['f' => 1.0, 'i' => 3]]))
                ->dropDuplicates(ref('f'))
                ->fetch()
                ->reduceToArray('i'),
        );
    }
}
