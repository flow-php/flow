<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\Text\TextExtractor;
use Flow\ETL\Adapter\Text\TextLoader;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\LocalFile;

class Text
{
    final public static function from(
        string|FileStream|array $stream,
        int $rows_in_batch = 1000,
        string $row_entry_name = 'row'
    ) : Extractor {
        if (\is_array($stream)) {
            $extractors = [];

            /** @var FileStream $file_stream */
            foreach ($stream as $file_stream) {
                $extractors[] = new TextExtractor(
                    $file_stream,
                    $rows_in_batch,
                    $row_entry_name,
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new TextExtractor(
            \is_string($stream) ? new LocalFile($stream) : $stream,
            $rows_in_batch,
            $row_entry_name,
        );
    }

    /**
     * @param FileStream|string $uri
     * @param bool $safe_mode - when set to true, stream or destination path will be used as a directory and output is going to be written into randomly generated file name
     * @param string $new_line_separator
     *
     * @return Loader
     */
    final public static function to(
        string|FileStream $uri,
        bool $safe_mode = false,
        string $new_line_separator = PHP_EOL
    ) : Loader {
        return new TextLoader(
            \is_string($uri) ? new LocalFile($uri) : $uri,
            $safe_mode,
            $new_line_separator
        );
    }
}
