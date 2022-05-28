<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\Parquet\Codename\ParquetExtractor;
use Flow\ETL\Adapter\Parquet\Codename\ParquetLoader;
use Flow\ETL\Exception\MissingDependencyException;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\LocalFile;

/**
 * @infection-ignore-all
 */
class Parquet
{
    /**
     * @param array<FileStream>|FileStream|string $uri
     * @param string $row_entry_name
     * @param array<string> $fields
     *
     * @return Extractor
     */
    final public static function from(
        string|FileStream|array $uri,
        string $row_entry_name = 'row',
        array $fields = []
    ) : Extractor {
        if (!\class_exists('codename\parquet\ParquetReader')) {
            throw new MissingDependencyException('Codename Parquet', 'codename/parquet');
        }

        if (\is_array($uri)) {
            $extractors = [];
            /** @var FileStream $filePath */
            foreach ($uri as $filePath) {
                $extractors[] = new ParquetExtractor(
                    $filePath,
                    $row_entry_name,
                    $fields
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new ParquetExtractor(
            \is_string($uri) ? new LocalFile($uri) : $uri,
            $row_entry_name,
            $fields
        );
    }

    /**
     * @param FileStream|string $uri
     * @param int $rows_in_group
     * @param bool $safe_mode
     * @param null|Schema $schema
     *
     * @throws MissingDependencyException
     *
     * @return Loader
     */
    final public static function to(
        string|FileStream $uri,
        int $rows_in_group = 1000,
        bool $safe_mode = false,
        Schema $schema = null
    ) : Loader {
        if (!\class_exists('codename\parquet\ParquetReader')) {
            throw new MissingDependencyException('Codename Parquet', 'codename/parquet');
        }

        return new ParquetLoader(
            \is_string($uri) ? new LocalFile($uri) : $uri,
            $rows_in_group,
            $safe_mode,
            $schema
        );
    }
}
