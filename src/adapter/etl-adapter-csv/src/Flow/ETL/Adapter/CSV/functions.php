<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Adapter\CSV\Detector\{Option, Options};
use Flow\ETL\Row\Schema;
use Flow\ETL\{Attribute\DocumentationDSL, Attribute\Module, Attribute\Type as DSLType, Extractor, Loader};
use Flow\Filesystem\{Path, SourceStream};

/**
 * @param int<1, max> $characters_read_in_line
 */
#[DocumentationDSL(module: Module::CSV, type: DSLType::EXTRACTOR)]
function from_csv(
    string|Path|array $path,
    bool $with_header = true,
    bool $empty_to_null = true,
    ?string $delimiter = null,
    ?string $enclosure = null,
    ?string $escape = null,
    int $characters_read_in_line = 1000,
    ?Schema $schema = null
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

#[DocumentationDSL(module: Module::CSV, type: DSLType::LOADER)]
function to_csv(
    string|Path $uri,
    bool $with_header = true,
    string $separator = ',',
    string $enclosure = '"',
    string $escape = '\\',
    string $new_line_separator = PHP_EOL,
    string $datetime_format = \DateTimeInterface::ATOM
) : Loader {
    return new CSVLoader(
        \is_string($uri) ? Path::realpath($uri) : $uri,
        $with_header,
        $separator,
        $enclosure,
        $escape,
        $new_line_separator,
        $datetime_format
    );
}

/**
 * @param SourceStream $stream - valid resource to CSV file
 * @param int<1, max> $lines - number of lines to read from CSV file, default 5, more lines means more accurate detection but slower detection
 * @param null|Option $fallback - fallback option to use when no best option can be detected, default is Option(',', '"', '\\')
 * @param null|Options $options - options to use for detection, default is Options::all()
 */
#[DocumentationDSL(module: Module::CSV, type: DSLType::HELPER)]
function csv_detect_separator(SourceStream $stream, int $lines = 5, ?Option $fallback = new Option(',', '"', '\\'), ?Options $options = null) : Option
{
    return (new CSVDetector($stream, $fallback, $options))->detect($lines);
}
