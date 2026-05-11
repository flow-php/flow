<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine\Arrow;

use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;

final class OptionsConverter
{
    /**
     * @return array<string, mixed>
     */
    public static function toExtension(Options $options): array
    {
        $result = [];

        $mappings = [
            [Option::ARROW_BATCH_SIZE,         'BATCH_SIZE'],
            [Option::GZIP_COMPRESSION_LEVEL,   'GZIP_COMPRESSION_LEVEL'],
            [Option::BROTLI_COMPRESSION_LEVEL, 'BROTLI_COMPRESSION_LEVEL'],
            [Option::ZSTD_COMPRESSION_LEVEL,   'ZSTD_COMPRESSION_LEVEL'],
            [Option::ROW_GROUP_SIZE_BYTES,     'ROW_GROUP_SIZE_BYTES'],
            [Option::PAGE_SIZE_BYTES,          'PAGE_SIZE_BYTES'],
            [Option::DICTIONARY_PAGE_SIZE,     'DICTIONARY_PAGE_SIZE'],
            [Option::WRITER_VERSION,           'WRITER_VERSION'],
        ];

        foreach ($mappings as [$option, $extensionKey]) {
            $value = $options->get($option);

            if ($value !== null) {
                $result[$extensionKey] = $value;
            }
        }

        $columnsCompressions = $options->getArray(Option::COLUMNS_COMPRESSIONS);

        if ($columnsCompressions !== null) {
            $mapped = [];

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

            foreach ($columnsEncodings as $path => $encoding) {
                if ($encoding instanceof Encodings) {
                    $mapped[$path] = $encoding->name;
                }
            }

            if ($mapped !== []) {
                $result['COLUMNS_ENCODINGS'] = $mapped;
            }
        }

        return $result;
    }
}
