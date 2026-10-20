<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Page;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\Tests\Context\ThriftBytes;
use Flow\Parquet\ThriftModel\DataPageHeader;
use Flow\Parquet\ThriftModel\PageHeader as ThriftPageHeader;
use PHPUnit\Framework\TestCase;

use function fopen;
use function ftell;
use function fwrite;
use function rewind;
use function strlen;

final class PageHeaderTest extends TestCase
{
    public function test_a_header_is_read_and_the_stream_left_after_it(): void
    {
        $bytes = ThriftBytes::of(new ThriftPageHeader([
            'type' => 0,
            'uncompressed_page_size' => 10,
            'compressed_page_size' => 8,
            'data_page_header' => new DataPageHeader([
                'num_values' => 2,
                'encoding' => 0,
                'definition_level_encoding' => 3,
                'repetition_level_encoding' => 3,
            ]),
        ]));
        $stream = fopen('php://memory', 'rb+') ?: static::fail('No memory stream');
        fwrite($stream, $bytes . 'page');
        rewind($stream);

        static::assertSame(8, PageHeader::read($stream, new Options())?->compressedPageSize());
        static::assertSame(strlen($bytes), ftell($stream));
    }

    public function test_no_header_is_null_and_keeps_the_position(): void
    {
        $stream = fopen('php://memory', 'rb+') ?: static::fail('No memory stream');
        fwrite($stream, "\xFF\xFF\xFF\xFF");
        rewind($stream);

        static::assertNull(PageHeader::read($stream, new Options()));
        static::assertSame(0, ftell($stream));
    }
}
