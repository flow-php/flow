<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Stream\FileStream;

class Json
{
    /**
     * @param array<FileStream>|FileStream|string $stream - string is internally turned into LocalFile stream
     * @param int $rows_in_batch
     * @param string $row_entry_name
     *
     * @return Extractor
     */
    public static function from(string|FileStream|array $stream, int $rows_in_batch = 1000, string $row_entry_name = 'row') : Extractor
    {
        if (\is_array($stream)) {
            $extractors = [];

            foreach ($stream as $file) {
                $extractors[] = new JsonExtractor(
                    $file,
                    $rows_in_batch,
                    $row_entry_name
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new JsonExtractor(
            $stream,
            $rows_in_batch,
            $row_entry_name
        );
    }

    /**
     * @param FileStream|string $stream
     * @param bool $safe_mode - when set to true, stream or destination path will be used as a directory and output is going to be written into randomly generated file name
     *
     * @return Loader
     */
    public static function to(string|FileStream $stream, bool $safe_mode = false) : Loader
    {
        return new JsonLoader($stream, $safe_mode);
    }
}
