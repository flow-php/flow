<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\CSV\League\CSVExtractor;
use Flow\ETL\Adapter\CSV\League\CSVLoader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;

class CSV
{
    /**
     * @throws InvalidArgumentException
     */
    final public static function from_file(
        string $file_name,
        int $rows_in_batch = 1000,
        ?int $header_offset = null,
        string $operation_mode = 'r',
        string $rowEntry_name = 'row',
        string $delimiter = ',',
        string $enclosure = '"',
        string $escape = '\\'
    ) : Extractor {
        if (!\class_exists('League\Csv\Reader')) {
            throw new InvalidArgumentException("Missing League CSV dependency, please run 'composer require league/csv'");
        }

        if (!\file_exists($file_name)) {
            throw new InvalidArgumentException("File {$file_name} not found.'");
        }

        return new CSVExtractor(
            $file_name,
            $rows_in_batch,
            $header_offset,
            $operation_mode,
            $rowEntry_name,
            $delimiter,
            $enclosure,
            $escape
        );
    }

    /**
     * @throws InvalidArgumentException
     */
    final public static function from_directory(
        string $folder_path,
        int $rows_in_batch = 1000,
        ?int $header_offset = null,
        string $operation_mode = 'r',
        string $rowEntry_name = 'row',
        string $delimiter = ',',
        string $enclosure = '"',
        string $escape = '\\'
    ) : Extractor {
        if (!\class_exists('League\Csv\Reader')) {
            throw new InvalidArgumentException("Missing League CSV dependency, please run 'composer require league/csv'");
        }

        if (!\file_exists($folder_path) || !\is_dir($folder_path)) {
            throw new InvalidArgumentException("Directory {$folder_path} not found.'");
        }

        $directoryIterator = new \RecursiveDirectoryIterator($folder_path);
        $directoryIterator->setFlags(\RecursiveDirectoryIterator::SKIP_DOTS);

        $regexIterator = new \RegexIterator(
            new \RecursiveIteratorIterator($directoryIterator),
            '/^.+\.csv$/i',
            \RecursiveRegexIterator::GET_MATCH
        );

        $extractors = [];

        /** @var array<string> $filePath */
        foreach ($regexIterator as $filePath) {
            $extractors[] = new CSVExtractor(
                /** @phpstan-ignore-next-line */
                \current($filePath),
                $rows_in_batch,
                $header_offset,
                $operation_mode,
                $rowEntry_name,
                $delimiter,
                $enclosure,
                $escape
            );
        }

        return new Extractor\ChainExtractor(...$extractors);
    }

    /**
     * @param string $file_name
     * @param string $open_mode
     * @param bool $with_header
     * @param string $delimiter
     * @param string $enclosure
     * @param string $escape
     *
     *@throws InvalidArgumentException
     *
     * @return Loader
     */
    final public static function to_file(
        string $file_name,
        string $open_mode = 'w+',
        bool $with_header = true,
        string $delimiter = ',',
        string $enclosure = '"',
        string $escape = '\\'
    ) : Loader {
        if (!\class_exists('League\Csv\Reader')) {
            throw new InvalidArgumentException("Missing League CSV dependency, please run 'composer require league/csv'");
        }

        return new CSVLoader($file_name, $open_mode, $with_header, false, $delimiter, $enclosure, $escape);
    }

    /**
     * @param string $file_name
     * @param string $open_mode
     * @param bool $with_header
     * @param string $delimiter
     * @param string $enclosure
     * @param string $escape
     *
     *@throws InvalidArgumentException
     *
     * @return Loader
     */
    final public static function to_directory(
        string $file_name,
        string $open_mode = 'w+',
        bool $with_header = true,
        string $delimiter = ',',
        string $enclosure = '"',
        string $escape = '\\'
    ) : Loader {
        if (!\class_exists('League\Csv\Reader')) {
            throw new InvalidArgumentException("Missing League CSV dependency, please run 'composer require league/csv'");
        }

        return new CSVLoader($file_name, $open_mode, true, $with_header, $delimiter, $enclosure, $escape);
    }
}
