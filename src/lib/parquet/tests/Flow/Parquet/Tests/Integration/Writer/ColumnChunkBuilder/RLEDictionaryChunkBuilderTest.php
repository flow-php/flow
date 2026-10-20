<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Writer\ColumnChunkBuilder;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Engine\RustParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\ColumnPageHeader;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_map;
use function extension_loaded;
use function iterator_to_array;

final class RLEDictionaryChunkBuilderTest extends TestCase
{
    /**
     * @param class-string $engine
     */
    #[TestWith([PhpParquetEngine::class])]
    #[TestWith([RustParquetEngine::class])]
    public function test_a_dictionary_column_over_many_pages_reads_back_exactly(string $engine): void
    {
        if ($engine === RustParquetEngine::class && !extension_loaded('arrow')) {
            static::markTestSkipped('arrow is not loaded');
        }

        $values = ['a', 'b', 'c', 'd', 'a', 'e'];

        static::assertSame($values, MemoryParquetFile::column(
            new Reader(engine: ParquetEngineMother::create($engine)),
            MemoryParquetFile::dictionaryColumn($values, new Options()),
            's',
        ));
    }

    /**
     * @param class-string $engine
     */
    #[TestWith([PhpParquetEngine::class])]
    #[TestWith([RustParquetEngine::class])]
    public function test_pages_past_the_dictionary_page_size_are_written_plain(string $engine): void
    {
        if ($engine === RustParquetEngine::class && !extension_loaded('arrow')) {
            static::markTestSkipped('arrow is not loaded');
        }

        $values = ['aaaa', 'bbbb', 'cccc', 'dddd', 'aaaa', 'eeee', null, 'ffff'];

        static::assertSame($values, MemoryParquetFile::column(
            new Reader(engine: ParquetEngineMother::create($engine)),
            MemoryParquetFile::dictionaryColumn($values, (new Options())->set(Option::DICTIONARY_PAGE_SIZE, 16)),
            's',
        ));
    }

    public function test_the_chunk_lists_both_encodings_after_a_fallback_to_plain(): void
    {
        $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::dictionaryColumn(
            ['aaaa', 'bbbb', 'cccc', 'dddd', 'eeee', 'ffff'],
            (new Options())->set(Option::DICTIONARY_PAGE_SIZE, 32),
        ));

        static::assertEqualsCanonicalizing(
            [Encodings::PLAIN, Encodings::RLE, Encodings::RLE_DICTIONARY],
            $file->metadata()->rowGroups()->all()[0]->columnChunks()[0]->encodings(),
        );
        static::assertSame(
            ['DICTIONARY_PAGE', 'RLE_DICTIONARY', 'RLE_DICTIONARY', 'PLAIN'],
            array_map(
                static fn(ColumnPageHeader $header): string => $header->pageHeader->type()->isDataPage()
                    ? $header->pageHeader->dataPageHeader()?->encoding()->name ?? ''
                    : $header->pageHeader->type()->name,
                iterator_to_array($file->pageHeaders(), false),
            ),
        );
    }

    public function test_page_headers_list_the_dictionary_page_and_every_data_page(): void
    {
        $file = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::dictionaryColumn([
            'a',
            'b',
            'c',
            'd',
            'a',
            'e',
        ], new Options()));

        static::assertSame(
            ['DICTIONARY_PAGE', 'DATA_PAGE', 'DATA_PAGE', 'DATA_PAGE'],
            array_map(
                static fn(ColumnPageHeader $header): string => $header->pageHeader->type()->name,
                iterator_to_array($file->pageHeaders(), false),
            ),
        );
    }
}
