<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\Parquet\Codename\ParquetExtractor;
use Flow\ETL\Adapter\Parquet\Codename\ParquetLoader;
use Flow\ETL\Exception\MissingDependencyException;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;

/**
 * @infection-ignore-all
 */
class Parquet
{
    /**
     * @param array<Path>|Path|string $uri
     * @param string $row_entry_name
     * @param array<string> $fields
     *
     * @return Extractor
     */
    final public static function from(
        string|Path|array $uri,
        string $row_entry_name = 'row',
        array $fields = []
    ) : Extractor {
        if (\is_array($uri)) {
            $extractors = [];

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
            \is_string($uri) ? Path::realpath($uri) : $uri,
            $row_entry_name,
            $fields
        );
    }

    /**
     * @param Path|string $path
     * @param int $rows_in_group
     * @param null|Schema $schema
     *
     *@throws MissingDependencyException
     *
     * @return Loader
     */
    final public static function to(
        string|Path $path,
        int $rows_in_group = 1000,
        Schema $schema = null
    ) : Loader {
        return new ParquetLoader(
            \is_string($path) ? Path::realpath($path) : $path,
            $rows_in_group,
            $schema
        );
    }
}
