<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Adapter\CSV\Detector\Option;
use Flow\ETL\Adapter\CSV\Detector\Options;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;

/**
 * @param int<0, max> $characters_read_in_line
 */
function from_csv(
    string|Path|array $path,
    bool $with_header = true,
    bool $empty_to_null = true,
    string|null $delimiter = null,
    string|null $enclosure = null,
    string|null $escape = null,
    int $characters_read_in_line = 1000,
    Schema|null $schema = null
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
                $schema
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
        $schema
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

/**
 * @param resource $resource - valid resource to CSV file opened with 'r' mode
 * @param int<1, max> $lines - number of lines to read from CSV file, default 5, more lines means more accurate detection but slower detection
 * @param null|Option $fallback - fallback option to use when no best option can be detected, default is Option(',', '"', '\\')
 * @param null|Options $options - options to use for detection, default is Options::all()
 */
function csv_detect_separator($resource, int $lines = 5, ?Option $fallback = new Option(',', '"', '\\'), ?Options $options = null) : Option
{
    return (new CSVDetector($resource, $fallback, $options))->detect($lines);
}
