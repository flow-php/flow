<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\JSON\JsonLoader;
use Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;

class Json
{
    /**
     * @param array<Path|string>|Path|string $path - string is internally turned into LocalFile stream
     * @param int $rows_in_batch
     * @param string $row_entry_name
     *
     * @return Extractor
     */
    public static function from(string|Path|array $path, int $rows_in_batch = 1000, string $row_entry_name = 'row') : Extractor
    {
        if (\is_array($path)) {
            $extractors = [];

            foreach ($path as $file) {
                $extractors[] = new JsonExtractor(
                    \is_string($file) ? Path::realpath($file) : $file,
                    $rows_in_batch,
                    $row_entry_name
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new JsonExtractor(
            \is_string($path) ? Path::realpath($path) : $path,
            $rows_in_batch,
            $row_entry_name
        );
    }

    /**
     * @param Path|string $path
     *
     * @return Loader
     */
    public static function to(string|Path $path) : Loader
    {
        return new JsonLoader(
            \is_string($path) ? Path::realpath($path) : $path,
        );
    }
}
