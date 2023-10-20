<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Adapter\CSV\CSVLoader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;

class CSV
{
    /**
     * @param array<Path|string>|Path|string $uri
     * @param int<0, max> $characters_read_in_line
     *
     * @throws InvalidArgumentException
     */
    final public static function from(
        string|Path|array $uri,
        int $rows_in_batch = 1000,
        bool $with_header = true,
        bool $empty_to_null = true,
        string $delimiter = ',',
        string $enclosure = '"',
        string $escape = '\\',
        int $characters_read_in_line = 1000
    ) : Extractor {
        if (\is_array($uri)) {
            $extractors = [];

            foreach ($uri as $file_uri) {
                $extractors[] = new CSVExtractor(
                    \is_string($file_uri) ? Path::realpath($file_uri) : $file_uri,
                    $rows_in_batch,
                    $with_header,
                    $empty_to_null,
                    $delimiter,
                    $enclosure,
                    $escape,
                    $characters_read_in_line,
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new CSVExtractor(
            \is_string($uri) ? Path::realpath($uri) : $uri,
            $rows_in_batch,
            $with_header,
            $empty_to_null,
            $delimiter,
            $enclosure,
            $escape,
            $characters_read_in_line,
        );
    }

    final public static function to(
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
}
