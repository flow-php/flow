<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\File;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\File\FileScan;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Double\RejectingFilter;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;

final class FileScanTest extends FlowTestCase
{
    public function test_a_default_scan_pushes_nothing_and_narrows_nothing(): void
    {
        $scan = new FileScan();

        static::assertNull($scan->limit);
        static::assertInstanceOf(OnlyFiles::class, $scan->pathFilter);
        static::assertSame([], $scan->conjuncts);
        static::assertFalse($scan->narrows());
    }

    #[TestWith([0])]
    #[TestWith([-1])]
    public function test_a_limit_below_one_is_refused(int $limit): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit must be greater than 0');

        new FileScan($limit);
    }

    public function test_a_filter_appends_its_conjuncts_in_push_order(): void
    {
        $first = ref('id')->greaterThan(lit(1));
        $second = ref('id')->lessThan(lit(10));
        $third = ref('id')->notEquals(lit(5));

        $scan = (new FileScan(5))
            ->withFilter($first)
            ->withFilter($second->and($third));

        static::assertSame([$first, $second, $third], $scan->conjuncts);
        static::assertSame(5, $scan->limit);
        static::assertTrue($scan->narrows());
    }

    public function test_a_second_limit_only_lowers_the_first_and_keeps_the_conjuncts(): void
    {
        $filter = ref('id')->greaterThan(lit(1));
        $scan = (new FileScan())
            ->withFilter($filter)
            ->withLimit(5);

        static::assertSame(3, $scan->withLimit(3)->limit);
        static::assertSame(5, $scan->withLimit(8)->limit);
        static::assertSame([$filter], $scan->withLimit(8)->conjuncts);
        static::assertTrue(
            (new FileScan())
                ->withLimit(1)
                ->narrows(),
        );
    }

    public function test_path_filters_compose_and_keep_the_conjuncts(): void
    {
        $filter = ref('id')->greaterThan(lit(1));
        $first = new RejectingFilter();
        $second = new RejectingFilter();

        $scan = (new FileScan())
            ->withFilter($filter)
            ->withPathFilter($first)
            ->withPathFilter($second);

        static::assertEquals(new Filters(new OnlyFiles(), $first, $second), $scan->pathFilter);
        static::assertSame([$filter], $scan->conjuncts);
        static::assertTrue(
            (new FileScan())
                ->withPathFilter($first)
                ->narrows(),
        );
    }
}
