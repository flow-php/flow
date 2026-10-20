<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\PageIndex;

use Flow\Parquet\ParquetFile\PageIndex\PageIndex;
use Flow\Parquet\Tests\Mother\PageIndexMother;
use PHPUnit\Framework\TestCase;

final class PageIndexTest extends TestCase
{
    public function test_a_column_index_and_offset_index_that_agree_are_a_page_index(): void
    {
        $pageIndex = PageIndex::of(PageIndexMother::int64([[0, 3], [4, 9]]), PageIndexMother::pagesAt(0, 4), 10);

        static::assertSame([1, 6], [$pageIndex?->pageOf(5), $pageIndex?->pageRows(1)]);
    }

    public function test_a_missing_index_is_no_page_index(): void
    {
        static::assertNull(PageIndex::of(null, PageIndexMother::pagesAt(0), 10));
        static::assertNull(PageIndex::of(PageIndexMother::int64([[0, 9]]), null, 10));
    }

    public function test_indexes_of_different_page_counts_are_no_page_index(): void
    {
        static::assertNull(PageIndex::of(PageIndexMother::int64([[0, 9]]), PageIndexMother::pagesAt(0, 5), 10));
    }
}
