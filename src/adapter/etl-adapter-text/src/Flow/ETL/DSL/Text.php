<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\Text\TextExtractor;
use Flow\ETL\Adapter\Text\TextLoader;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Factory\NativeEntryFactory;

class Text
{
    /**
     * @param array<Path|string>|Path|string $path
     * @param int $rows_in_batch
     *
     * @return Extractor
     */
    final public static function from(
        string|Path|array $path,
        int $rows_in_batch = 1000,
        EntryFactory $entry_factory = new NativeEntryFactory()
    ) : Extractor {
        if (\is_array($path)) {
            $extractors = [];

            foreach ($path as $file_path) {
                $extractors[] = new TextExtractor(
                    \is_string($file_path) ? Path::realpath($file_path) : $file_path,
                    $rows_in_batch,
                    $entry_factory
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new TextExtractor(
            \is_string($path) ? Path::realpath($path) : $path,
            $rows_in_batch,
            $entry_factory
        );
    }

    /**
     * @param Path|string $path
     * @param string $new_line_separator
     *
     * @return Loader
     */
    final public static function to(
        string|Path $path,
        string $new_line_separator = PHP_EOL
    ) : Loader {
        return new TextLoader(
            \is_string($path) ? Path::realpath($path) : $path,
            $new_line_separator
        );
    }
}
