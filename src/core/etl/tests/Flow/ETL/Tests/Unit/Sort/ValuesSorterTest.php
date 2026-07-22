<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Sort\ValuesSorter;
use Flow\ETL\Tests\FlowTestCase;

use function array_keys;
use function array_values;

final class ValuesSorterTest extends FlowTestCase
{
    public function test_sorts_integers(): void
    {
        static::assertSame([1, 2, 3], array_values(ValuesSorter::sort([3, 1, 2], SortOrder::ASC)));
        static::assertSame([3, 2, 1], array_values(ValuesSorter::sort([3, 1, 2], SortOrder::DESC)));
    }

    public function test_sorts_numeric_strings_numerically(): void
    {
        static::assertSame(['9', '10', '100'], array_values(ValuesSorter::sort(['100', '9', '10'], SortOrder::ASC)));
    }

    public function test_sorts_mixed_ints_and_floats(): void
    {
        static::assertSame([1, 1.5, 2], array_values(ValuesSorter::sort([2, 1.5, 1], SortOrder::ASC)));
    }

    public function test_sorts_strings_bytewise(): void
    {
        static::assertSame(['a', 'b', 'c'], array_values(ValuesSorter::sort(['c', 'a', 'b'], SortOrder::ASC)));
        static::assertSame(['c', 'b', 'a'], array_values(ValuesSorter::sort(['c', 'a', 'b'], SortOrder::DESC)));
    }

    public function test_sorts_datetimes_chronologically(): void
    {
        $early = new DateTimeImmutable('2024-01-01 00:00:00');
        $late = new DateTimeImmutable('2024-06-01 00:00:00');
        $mid = new DateTimeImmutable('2024-03-01 00:00:00');

        static::assertSame(
            [$early, $mid, $late],
            array_values(ValuesSorter::sort([$late, $early, $mid], SortOrder::ASC)),
        );
        static::assertSame(
            [$late, $mid, $early],
            array_values(ValuesSorter::sort([$late, $early, $mid], SortOrder::DESC)),
        );
    }

    public function test_sorts_datetimes_with_microsecond_precision(): void
    {
        $first = new DateTimeImmutable('2024-01-01 00:00:00.000001');
        $second = new DateTimeImmutable('2024-01-01 00:00:00.000002');

        static::assertSame([$first, $second], array_values(ValuesSorter::sort([$second, $first], SortOrder::ASC)));
    }

    public function test_sorts_pre_epoch_datetimes(): void
    {
        $preEpoch = new DateTimeImmutable('1969-12-31 23:59:59.500000');
        $epoch = new DateTimeImmutable('1970-01-01 00:00:00');

        static::assertSame([$preEpoch, $epoch], array_values(ValuesSorter::sort([$epoch, $preEpoch], SortOrder::ASC)));
    }

    public function test_sorts_date_intervals_through_the_fallback(): void
    {
        $short = new DateInterval('PT1H');
        $long = new DateInterval('PT5H');

        static::assertSame([$short, $long], array_values(ValuesSorter::sort([$long, $short], SortOrder::ASC)));
        static::assertSame([$long, $short], array_values(ValuesSorter::sort([$long, $short], SortOrder::DESC)));
    }

    public function test_sorts_arrays_through_the_fallback(): void
    {
        static::assertSame([[1], [2]], array_values(ValuesSorter::sort([[2], [1]], SortOrder::ASC)));
    }

    public function test_mixed_types_keep_their_order(): void
    {
        static::assertSame([1, 'a', null], array_values(ValuesSorter::sort([1, 'a', null], SortOrder::ASC)));
    }

    public function test_null_values_keep_their_order(): void
    {
        static::assertSame([null, null], array_values(ValuesSorter::sort([null, null], SortOrder::ASC)));
    }

    public function test_keys_are_preserved(): void
    {
        static::assertSame([2, 0, 1], array_keys(ValuesSorter::sort([5, 7, 1], SortOrder::ASC)));
    }

    public function test_empty_values_stay_empty(): void
    {
        static::assertSame([], ValuesSorter::sort([], SortOrder::ASC));
    }
}
