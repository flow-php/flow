<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Page\Header;

use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Statistics;
use PHPUnit\Framework\TestCase;

use function pack;

final class DataPageHeaderTest extends TestCase
{
    public function test_page_statistics_are_kept_through_thrift(): void
    {
        $header = DataPageHeader::fromThrift(
            (new DataPageHeader(
                Encodings::PLAIN,
                Encodings::RLE,
                Encodings::RLE,
                2,
                new Statistics(null, null, 1, null, pack('P', 9), pack('P', 1)),
            ))->toThrift(),
        );

        static::assertSame(1, $header->statistics()?->minValue(FlatColumn::int64('id')));
        static::assertSame(9, $header->statistics()?->maxValue(FlatColumn::int64('id')));
        static::assertSame(1, $header->statistics()?->nullCount());
    }

    public function test_a_page_without_statistics_has_none(): void
    {
        static::assertNull(
            DataPageHeader::fromThrift(
                (new DataPageHeader(Encodings::PLAIN, Encodings::RLE, Encodings::RLE, 2))->toThrift(),
            )->statistics(),
        );
    }
}
