<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;

/**
 * @param int<0, max> $characters_read_in_line
 */
function from_csv(
    string|Path|array $path,
    bool $with_header = true,
    bool $empty_to_null = true,
    string $delimiter = ',',
    string $enclosure = '"',
    string $escape = '\\',
    int $characters_read_in_line = 1000
) : Extractor {
    if (\is_array($path)) {
        $extractors = [];

        foreach ($path as $file_path) {
            $extractors[] = new CSVExtractor(
                \is_string($file_path) ? Path::realpath($file_path) : $file_path,
                $with_header,
                $empty_to_null,
                $delimiter,
                $enclosure,
                $escape,
                $characters_read_in_line,
            );
        }

        return from_all(...$extractors);
    }

    return new CSVExtractor(
        \is_string($path) ? Path::realpath($path) : $path,
        $with_header,
        $empty_to_null,
        $delimiter,
        $enclosure,
        $escape,
        $characters_read_in_line,
    );
}

function to_csv(
    string|Path $uri,
    bool $with_header = true,
    string $separator = ',',
    string $enclosure = '"',
    string $escape = '\\',
    string $new_line_separator = PHP_EOL
) : Loader {
    return new CSVLoader(
        \is_string($uri) ? Path::realpath($uri) : $uri,
        $with_header,
        $separator,
        $enclosure,
        $escape,
        $new_line_separator
    );
}
