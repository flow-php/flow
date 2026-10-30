<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileSplit;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\Filesystem\DSL\path;

final class CriteriaTest extends FlowTestCase
{
    public function test_default_criteria_push_nothing_and_narrow_nothing(): void
    {
        $criteria = new Criteria();

        static::assertNull($criteria->limit);
        static::assertSame(0, $criteria->offset);
        static::assertInstanceOf(OnlyFiles::class, $criteria->pathFilter);
        static::assertSame([], $criteria->conjuncts);
        static::assertNull($criteria->split);
        static::assertFalse($criteria->narrows());
    }

    #[TestWith([0])]
    #[TestWith([-1])]
    public function test_a_limit_below_one_is_refused(int $limit): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit must be greater than 0');

        new Criteria($limit);
    }

    public function test_a_filter_appends_its_conjuncts_in_push_order(): void
    {
        $first = ref('id')->greaterThan(lit(1));
        $second = ref('id')->lessThan(lit(10));
        $third = ref('id')->notEquals(lit(5));

        $criteria = (new Criteria(5))
            ->withFilter($first)
            ->withFilter($second->and($third));

        static::assertSame([$first, $second, $third], $criteria->conjuncts);
        static::assertSame(5, $criteria->limit);
        static::assertTrue($criteria->narrows());
    }

    public function test_a_second_limit_only_lowers_the_first_and_keeps_the_conjuncts(): void
    {
        $filter = ref('id')->greaterThan(lit(1));
        $criteria = (new Criteria())
            ->withFilter($filter)
            ->withLimit(5);

        static::assertSame(3, $criteria->withLimit(3)->limit);
        static::assertSame(5, $criteria->withLimit(8)->limit);
        static::assertSame([$filter], $criteria->withLimit(8)->conjuncts);
        static::assertTrue(
            (new Criteria())
                ->withLimit(1)
                ->narrows(),
        );
    }

    public function test_path_filters_compose_and_keep_the_conjuncts(): void
    {
        $filter = ref('id')->greaterThan(lit(1));
        $first = new RejectingFilter();
        $second = new RejectingFilter();

        $criteria = (new Criteria())
            ->withFilter($filter)
            ->withPathFilter($first)
            ->withPathFilter($second);

        static::assertEquals(new Filters(new OnlyFiles(), $first, $second), $criteria->pathFilter);
        static::assertSame([$filter], $criteria->conjuncts);
        static::assertTrue(
            (new Criteria())
                ->withPathFilter($first)
                ->narrows(),
        );
    }

    public function test_a_split_narrows(): void
    {
        $criteria = (new Criteria())->withSplit(new FileSplit(path('memory://orders.csv'), 0, 1_024));

        static::assertTrue($criteria->narrows());
        static::assertSame(0, $criteria->split?->start);
    }

    public function test_with_split_keeps_limit_path_filter_and_conjuncts_and_every_push_keeps_the_split(): void
    {
        $filter = ref('id')->greaterThan(lit(1));
        $pathFilter = new RejectingFilter();
        $split = new FileSplit(path('memory://orders.csv'), 1_024, 1_024);

        $criteria = (new Criteria(5))
            ->withFilter($filter)
            ->withPathFilter($pathFilter)
            ->withSplit($split);

        static::assertSame(5, $criteria->limit);
        static::assertSame([$filter], $criteria->conjuncts);
        static::assertEquals(new Filters(new OnlyFiles(), $pathFilter), $criteria->pathFilter);
        static::assertSame($split, $criteria->split);
        static::assertSame($split, $criteria->withLimit(3)->split);
        static::assertSame($split, $criteria->withFilter(ref('id')->lessThan(lit(9)))->split);
        static::assertSame($split, $criteria->withPathFilter(new RejectingFilter())->split);
    }

    public function test_an_offset_below_zero_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0, given: -1');

        new Criteria(offset: -1);
    }

    public function test_an_offset_narrows(): void
    {
        static::assertTrue((new Criteria(offset: 1))->narrows());
    }

    public function test_offsets_add_up_and_keep_limit_path_filter_and_conjuncts(): void
    {
        $filter = ref('id')->greaterThan(lit(1));
        $pathFilter = new RejectingFilter();

        $criteria = (new Criteria(5))
            ->withFilter($filter)
            ->withPathFilter($pathFilter)
            ->withOffset(3)
            ->withOffset(4);

        static::assertSame(7, $criteria->offset);
        static::assertSame(5, $criteria->limit);
        static::assertSame([$filter], $criteria->conjuncts);
        static::assertEquals(new Filters(new OnlyFiles(), $pathFilter), $criteria->pathFilter);
        static::assertSame(7, $criteria->withLimit(2)->offset);
        static::assertSame(7, $criteria->withFilter(ref('id')->lessThan(lit(9)))->offset);
        static::assertSame(7, $criteria->withPathFilter(new RejectingFilter())->offset);
    }

    public function test_a_negative_offset_push_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0, given: -1');

        (new Criteria(offset: 5))->withOffset(-1);
    }

    public function test_a_read_with_an_offset_cannot_be_split(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A read with an offset cannot be split, given offset: 3');

        (new Criteria(offset: 3))->withSplit(new FileSplit(path('memory://orders.csv'), 0, 1_024));
    }

    public function test_a_split_read_refuses_an_offset(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A read with an offset cannot be split, given offset: 3');

        (new Criteria())
            ->withSplit(new FileSplit(path('memory://orders.csv'), 0, 1_024))
            ->withOffset(3);
    }
}
