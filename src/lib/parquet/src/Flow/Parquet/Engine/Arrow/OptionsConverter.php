<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine\Arrow;

use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\Writer\StatisticsLevel;

use function array_map;

final class OptionsConverter
{
    // called by name from arrow-ext RustParquetEngine::__construct(), pinned by phpts 047, 052
    /**
     * The options the arrow engine itself reads, which Rust cannot read off the Option enum.
     *
     * @return array{INT_96_AS_DATETIME: bool, ARROW_WRITE_BATCH_SIZE: int}
     */
    public static function toEngine(?Options $options): array
    {
        $options ??= new Options();

        return [
            'INT_96_AS_DATETIME' => $options->getBool(Option::INT_96_AS_DATETIME),
            'ARROW_WRITE_BATCH_SIZE' => $options->getInt(Option::ARROW_WRITE_BATCH_SIZE),
        ];
    }

    // called by name from arrow-ext RustParquetEngine::openForWrite(), pinned by phpts 047, 052
    /**
     * @return array<string, mixed>
     */
    public static function toExtension(Options $options): array
    {
        $result = [];

        $mappings = [
            [Option::GZIP_COMPRESSION_LEVEL,       'GZIP_COMPRESSION_LEVEL'],
            [Option::BROTLI_COMPRESSION_LEVEL,     'BROTLI_COMPRESSION_LEVEL'],
            [Option::ZSTD_COMPRESSION_LEVEL,       'ZSTD_COMPRESSION_LEVEL'],
            [Option::ROW_GROUP_SIZE_BYTES,         'ROW_GROUP_SIZE_BYTES'],
            [Option::PAGE_SIZE_BYTES,              'PAGE_SIZE_BYTES'],
            [Option::PAGE_MAXIMUM_ROWS_COUNT,      'PAGE_MAXIMUM_ROWS_COUNT'],
            [Option::DICTIONARY_ENABLED,           'DICTIONARY_ENABLED'],
            [Option::DICTIONARY_PAGE_SIZE,         'DICTIONARY_PAGE_SIZE'],
            [Option::WRITER_VERSION,               'WRITER_VERSION'],
            [Option::STATISTICS_TRUNCATE_LENGTH,   'STATISTICS_TRUNCATE_LENGTH'],
            [Option::COLUMN_INDEX_TRUNCATE_LENGTH, 'COLUMN_INDEX_TRUNCATE_LENGTH'],
            [Option::PAGE_HEADER_STATISTICS,       'PAGE_HEADER_STATISTICS'],
        ];

        foreach ($mappings as [$option, $extensionKey]) {
            $value = $options->get($option);

            if ($value !== null) {
                $result[$extensionKey] = $value;
            }
        }

        $result['STATISTICS_LEVEL'] = $options->getEnum(Option::STATISTICS_LEVEL, StatisticsLevel::class)->name;

        $columnsCompressions = $options->getArray(Option::COLUMNS_COMPRESSIONS);

        if ($columnsCompressions !== null) {
            $mapped = [];

            // Options::getArray returns array<mixed>; each entry is narrowed via instanceof.
            // @mago-ignore analysis:mixed-assignment
            foreach ($columnsCompressions as $path => $compression) {
                if ($compression instanceof Compressions) {
                    $mapped[$path] = $compression->name;
                }
            }

            if ($mapped !== []) {
                $result['COLUMNS_COMPRESSIONS'] = $mapped;
            }
        }

        $columnsEncodings = $options->getArray(Option::COLUMNS_ENCODINGS);

        if ($columnsEncodings !== null) {
            $mapped = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($columnsEncodings as $path => $encoding) {
                if ($encoding instanceof Encodings) {
                    $mapped[$path] = $encoding->name;
                }
            }

            if ($mapped !== []) {
                $result['COLUMNS_ENCODINGS'] = $mapped;
            }
        }

        $bloomFilters = BloomFilterOptions::forColumns($options);

        if ($bloomFilters !== []) {
            $result['COLUMNS_BLOOM_FILTERS'] = array_map(static fn(BloomFilterOptions $bloomFilter): array => [
                'fpp' => $bloomFilter->fpp,
                'ndv' => $bloomFilter->ndv,
            ], $bloomFilters);
        }

        return $result;
    }
}
