<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\XML\XMLReaderExtractor;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;

class XML
{
    final public static function from_file(
        string $file_name,
        string $xml_node_path = '',
        int $rows_in_batch = 1000,
        string $row_entry_name = 'row'
    ) : Extractor {
        if (!\file_exists($file_name)) {
            throw new InvalidArgumentException("File {$file_name} not found.'");
        }

        return new XMLReaderExtractor(
            $file_name,
            $xml_node_path,
            $rows_in_batch,
            $row_entry_name
        );
    }

    final public static function from_directory(
        string $folder_path,
        string $xml_node_path = '',
        int $rows_in_batch = 1000,
        string $row_entry_name = 'row'
    ) : Extractor {
        if (!\file_exists($folder_path) || !\is_dir($folder_path)) {
            throw new InvalidArgumentException("Directory {$folder_path} not found.'");
        }

        $directoryIterator = new \RecursiveDirectoryIterator($folder_path);
        $directoryIterator->setFlags(\RecursiveDirectoryIterator::SKIP_DOTS);

        $regexIterator = new \RegexIterator(
            new \RecursiveIteratorIterator($directoryIterator),
            '/^.+\.xml$/i',
            \RecursiveRegexIterator::GET_MATCH
        );

        $extractors = [];

        /** @var array<string> $filePath */
        foreach ($regexIterator as $filePath) {
            $extractors[] = new XMLReaderExtractor(
                /** @phpstan-ignore-next-line */
                \current($filePath),
                $xml_node_path,
                $rows_in_batch,
                $row_entry_name
            );
        }

        return new Extractor\ChainExtractor(...$extractors);
    }
}
