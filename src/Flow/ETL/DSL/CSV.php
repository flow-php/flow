<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Adapter\CSV\CSVLoader;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\LocalFile;

class CSV
{
    final public static function from(
        string|FileStream|array $stream,
        int $rows_in_batch = 1000,
        bool $with_header = true,
        bool $empty_to_null = true,
        string $row_entry_name = 'row',
        string $delimiter = ',',
        string $enclosure = '"',
        string $escape = '\\'
    ) : Extractor {
        if (\is_array($stream)) {
            $extractors = [];

            /** @var FileStream $file_stream */
            foreach ($stream as $file_stream) {
                $extractors[] = new CSVExtractor(
                    $file_stream,
                    $rows_in_batch,
                    $with_header,
                    $empty_to_null,
                    $row_entry_name,
                    $delimiter,
                    $enclosure,
                    $escape
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new CSVExtractor(
            \is_string($stream) ? new LocalFile($stream) : $stream,
            $rows_in_batch,
            $with_header,
            $empty_to_null,
            $row_entry_name,
            $delimiter,
            $enclosure,
            $escape
        );
    }

    /**
     * @param FileStream|string $uri
     * @param bool $with_header
     * @param bool $safe_mode - when set to true, stream or destination path will be used as a directory and output is going to be written into randomly generated file name
     * @param string $separator
     * @param string $enclosure
     * @param string $escape
     * @param string $new_line_separator
     *
     * @return Loader
     */
    final public static function to(
        string|FileStream $uri,
        bool $with_header = true,
        bool $safe_mode = false,
        string $separator = ',',
        string $enclosure = '"',
        string $escape = '\\',
        string $new_line_separator = PHP_EOL
    ) : Loader {
        return new CSVLoader(
            \is_string($uri) ? new LocalFile($uri) : $uri,
            $with_header,
            $safe_mode,
            $separator,
            $enclosure,
            $escape,
            $new_line_separator
        );
    }
}
