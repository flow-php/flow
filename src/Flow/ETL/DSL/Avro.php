<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\Avro\FlixTech\AvroExtractor;
use Flow\ETL\Adapter\Avro\FlixTech\AvroLoader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;

/**
 * @infection-ignore-all
 */
class Avro
{
    /**
     * @param string $file_name
     * @param int $rows_in_batch
     * @param string $row_entry_name
     *
     * @throws InvalidArgumentException
     *
     * @return Extractor
     */
    final public static function from_file(
        string $file_name,
        int $rows_in_batch = 1000,
        string $row_entry_name = 'row'
    ) : Extractor {
        if (!\class_exists('AvroDataIO')) {
            throw new InvalidArgumentException("Missing Flix Tech Avro dependency, please run 'composer require flix-tech/avro-php'");
        }

        if (!\file_exists($file_name)) {
            throw new InvalidArgumentException("File {$file_name} not found.'");
        }

        return new AvroExtractor($file_name, $rows_in_batch, $row_entry_name);
    }

    /**
     * @param string $folder_path
     * @param int $rows_in_batch
     * @param string $row_entry_name
     *
     * @throws InvalidArgumentException
     *
     * @return Extractor
     */
    final public static function from_directory(
        string $folder_path,
        int $rows_in_batch = 1000,
        string $row_entry_name = 'row',
    ) : Extractor {
        if (!\class_exists('AvroDataIO')) {
            throw new InvalidArgumentException("Missing Flix Tech Avro dependency, please run 'composer require flix-tech/avro-php'");
        }

        if (!\file_exists($folder_path) || !\is_dir($folder_path)) {
            throw new InvalidArgumentException("Directory {$folder_path} not found.'");
        }

        $directoryIterator = new \RecursiveDirectoryIterator($folder_path);
        $directoryIterator->setFlags(\RecursiveDirectoryIterator::SKIP_DOTS);

        $regexIterator = new \RegexIterator(
            new \RecursiveIteratorIterator($directoryIterator),
            '/^.+\.avro$/i',
            \RecursiveRegexIterator::GET_MATCH
        );

        $extractors = [];

        /** @var array<string> $filePath */
        foreach ($regexIterator as $filePath) {
            /** @phpstan-ignore-next-line */
            $extractors[] = new AvroExtractor(\current($filePath), $rows_in_batch, $row_entry_name);
        }

        return new Extractor\ChainExtractor(...$extractors);
    }

    /**
     * @param string $file_name
     * @param null|Schema $schema
     *
     * @throws InvalidArgumentException
     *
     * @return Loader
     */
    final public static function to_file(string $file_name, Schema $schema = null) : Loader
    {
        if (!\class_exists('AvroDataIO')) {
            throw new InvalidArgumentException("Missing Flix Tech Avro dependency, please run 'composer require flix-tech/avro-php'");
        }

        return new AvroLoader($file_name, false);
    }

    /**
     * @param string $file_name
     * @param null|Schema $schema
     *
     * @throws InvalidArgumentException
     *
     * @return Loader
     */
    final public static function to_directory(string $file_name, Schema $schema = null) : Loader
    {
        if (!\class_exists('AvroDataIO')) {
            throw new InvalidArgumentException("Missing Flix Tech Avro dependency, please run 'composer require flix-tech/avro-php'");
        }

        return new AvroLoader($file_name, true, $schema);
    }
}
