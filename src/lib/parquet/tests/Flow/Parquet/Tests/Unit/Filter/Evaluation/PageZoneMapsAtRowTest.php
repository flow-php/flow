<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Filter\Evaluation;

use Flow\Parquet\Filter\Evaluation\PageZoneMaps;
use Flow\Parquet\Filter\Evaluation\PageZoneMapsAtRow;
use Flow\Parquet\ParquetFile\MetadataRowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Mother\FooterMother;
use Flow\Parquet\Tests\Mother\PageIndexMother;
use PHPUnit\Framework\TestCase;

final class PageZoneMapsAtRowTest extends TestCase
{
    public function test_a_view_answers_with_the_page_holding_its_row_and_shares_the_decoded_page(): void
    {
        $pages = new PageZoneMaps(
            new MetadataRowGroupChunks(FooterMother::oneGroup(4, $a = FlatColumn::int64('a'))->metadata()),
            ['a' => PageIndexMother::consistent(
                PageIndexMother::int64([[0, 1], [2, 3]]),
                PageIndexMother::pagesAt(0, 2),
                4,
            )],
        );

        static::assertSame(2, (new PageZoneMapsAtRow($pages, 3))->zone($a)?->min);
        static::assertSame($pages->zone($a, 0), (new PageZoneMapsAtRow($pages, 1))->zone($a));
    }
}
