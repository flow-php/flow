<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Scan;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Tests\Double\RejectingFilter;

final class ScanTest extends FlowTestCase
{
    public function test_a_default_scan_pushes_no_limit_and_lists_only_files(): void
    {
        $scan = new Scan();

        static::assertNull($scan->limit);
        static::assertInstanceOf(OnlyFiles::class, $scan->pathFilter);
    }

    public function test_limit_and_path_filter_are_the_values_it_was_built_with(): void
    {
        $filter = new RejectingFilter();

        $scan = new Scan(5, $filter);

        static::assertSame(5, $scan->limit);
        static::assertSame($filter, $scan->pathFilter);
    }

    public function test_with_limit_narrows_and_never_widens(): void
    {
        static::assertSame(10, (new Scan())->withLimit(10)->withLimit(100)->limit);
        static::assertSame(10, (new Scan())->withLimit(100)->withLimit(10)->limit);
    }

    public function test_with_limit_refuses_zero_or_less(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit must be greater than 0');

        (new Scan())->withLimit(0);
    }

    public function test_with_path_filter_composes_with_the_filters_already_held(): void
    {
        $first = new RejectingFilter();
        $second = new RejectingFilter();

        $scan = (new Scan(limit: 3))
            ->withPathFilter($first)
            ->withPathFilter($second);

        static::assertSame(3, $scan->limit);
        static::assertEquals(new Filters(new OnlyFiles(), $first, $second), $scan->pathFilter);
    }
}
