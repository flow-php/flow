<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Adapter\CSV\Detector\{Option, Options};
use Flow\ETL\Row\Schema;
use Flow\ETL\{Attribute\DocumentationDSL,
    Attribute\DocumentationExample,
    Attribute\Module,
    Attribute\Type as DSLType
};
use Flow\Filesystem\{Path, SourceStream};

/**
 * @param Path|string $path
 * @param bool $empty_to_null - @deprecated use $loader->withEmptyToNull() instead
 * @param bool $with_header - @deprecated use $loader->withHeader() instead
 * @param null|string $separator - @deprecated use $loader->withSeparator() instead
 * @param null|string $enclosure - @deprecated use $loader->withEnclosure() instead
 * @param null|string $escape - @deprecated use $loader->withEscape() instead
 * @param int<1, max> $characters_read_in_line - @deprecated use $loader->withCharactersReadInLine() instead
 * @param null|Schema $schema - @deprecated use $loader->withSchema() instead
 */
#[DocumentationDSL(module: Module::CSV, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'data_reading', example: 'csv')]
function from_csv(
    string|Path $path,
    bool $with_header = true,
    bool $empty_to_null = true,
    ?string $separator = null,
    ?string $enclosure = null,
    ?string $escape = null,
    int $characters_read_in_line = 1000,
    ?Schema $schema = null,
) : CSVExtractor {

    $loader = (new CSVExtractor(\is_string($path) ? Path::realpath($path) : $path))
        ->withHeader($with_header)
        ->withEmptyToNull($empty_to_null)
        ->withCharactersReadInLine($characters_read_in_line);

    if ($separator !== null) {
        $loader->withSeparator($separator);
    }

    if ($enclosure !== null) {
        $loader->withEnclosure($enclosure);
    }

    if ($escape !== null) {
        $loader->withEscape($escape);
    }

    if ($schema !== null) {
        $loader->withSchema($schema);
    }

    return $loader;
}

/**
 * @param Path|string $uri
 * @param bool $with_header - @deprecated use $loader->withHeader() instead
 * @param string $separator - @deprecated use $loader->withSeparator() instead
 * @param string $enclosure - @deprecated use $loader->withEnclosure() instead
 * @param string $escape - @deprecated use $loader->withEscape() instead
 * @param string $new_line_separator - @deprecated use $loader->withNewLineSeparator() instead
 * @param string $datetime_format - @deprecated use $loader->withDateTimeFormat() instead
 */
#[DocumentationDSL(module: Module::CSV, type: DSLType::LOADER)]
function to_csv(
    string|Path $uri,
    bool $with_header = true,
    string $separator = ',',
    string $enclosure = '"',
    string $escape = '\\',
    string $new_line_separator = PHP_EOL,
    string $datetime_format = \DateTimeInterface::ATOM,
) : CSVLoader {
    return (new CSVLoader(\is_string($uri) ? Path::realpath($uri) : $uri))
        ->withHeader($with_header)
        ->withSeparator($separator)
        ->withEnclosure($enclosure)
        ->withEscape($escape)
        ->withNewLineSeparator($new_line_separator)
        ->withDateTimeFormat($datetime_format);
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
