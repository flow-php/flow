<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\Parquet\Codename\ParquetExtractor;
use Flow\ETL\Adapter\Parquet\Codename\ParquetLoader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;

/**
 * @infection-ignore-all
 */
class Parquet
{
    /**
     * @param string $file_name
     * @param string $row_entry_name
     * @param array<string> $fields
     *
     * @throws InvalidArgumentException
     *
     * @return Extractor
     */
    final public static function from_file(
        string $file_name,
        string $row_entry_name = 'row',
        array $fields = []
    ) : Extractor {
        if (!\class_exists('codename\parquet\ParquetReader')) {
            throw new InvalidArgumentException("Missing Codename Parquet dependency, please run 'composer require codename/parquet'");
        }

        if (!\file_exists($file_name)) {
            throw new InvalidArgumentException("File {$file_name} not found.'");
        }

        return new ParquetExtractor($file_name, $row_entry_name, $fields);
    }

    /**
     * @param string $folder_path
     * @param string $row_entry_name
     * @param array<string> $fields - only given fields will be read from the file
     *
     * @throws InvalidArgumentException
     *
     * @return Extractor
     */
    final public static function from_directory(
        string $folder_path,
        string $row_entry_name = 'row',
        array $fields = []
    ) : Extractor {
        if (!\class_exists('codename\parquet\ParquetReader')) {
            throw new InvalidArgumentException("Missing Codename Parquet dependency, please run 'composer require codename/parquet'");
        }

        if (!\file_exists($folder_path) || !\is_dir($folder_path)) {
            throw new InvalidArgumentException("Directory {$folder_path} not found.'");
        }

        $directoryIterator = new \RecursiveDirectoryIterator($folder_path);
        $directoryIterator->setFlags(\RecursiveDirectoryIterator::SKIP_DOTS);

        $regexIterator = new \RegexIterator(
            new \RecursiveIteratorIterator($directoryIterator),
            '/^.+\.parquet$/i',
            \RecursiveRegexIterator::GET_MATCH
        );

        $extractors = [];

        /** @var array<string> $filePath */
        foreach ($regexIterator as $filePath) {
            /** @phpstan-ignore-next-line */
            $extractors[] = new ParquetExtractor(\current($filePath), $row_entry_name, $fields);
        }

        return new Extractor\ChainExtractor(...$extractors);
    }

    /**
     * @param string $file_name
     * @param int $rows_in_group
     *
     * @throws InvalidArgumentException
     *
     * @return Loader
     */
    final public static function to_file(
        string $file_name,
        int $rows_in_group = 1000
    ) : Loader {
        if (!\class_exists('codename\parquet\ParquetReader')) {
            throw new InvalidArgumentException("Missing Codename Parquet dependency, please run 'composer require codename/parquet'");
        }

        return new ParquetLoader($file_name, $rows_in_group, false);
    }

    /**
     * @param string $file_name
     * @param int $rows_in_group
     *
     * @throws InvalidArgumentException
     *
     * @return Loader
     */
    final public static function to_directory(
        string $file_name,
        int $rows_in_group = 1000
    ) : Loader {
        if (!\class_exists('codename\parquet\ParquetReader')) {
            throw new InvalidArgumentException("Missing Codename Parquet dependency, please run 'composer require codename/parquet'");
        }

        return new ParquetLoader($file_name, $rows_in_group, true);
    }
}
