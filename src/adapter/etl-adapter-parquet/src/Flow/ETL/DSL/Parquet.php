<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use codename\parquet\ParquetOptions;
use Flow\ETL\Adapter\Parquet\Codename\ParquetLoader;
use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\ETL\Exception\MissingDependencyException;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\Options;

/**
 * @infection-ignore-all
 */
class Parquet
{
    /**
     * @param array<Path>|Path|string $uri
     * @param array<string> $fields
     *
     * @return Extractor
     */
    final public static function from(
        string|Path|array $uri,
        array $fields = [],
        Options $options = new Options(),
        ByteOrder $byte_order = ByteOrder::LITTLE_ENDIAN,
        int $rows_in_batch = 1000,
    ) : Extractor {
        if (\is_array($uri)) {
            $extractors = [];

            foreach ($uri as $filePath) {
                $extractors[] = new ParquetExtractor(
                    $filePath,
                    $options,
                    $byte_order,
                    $fields,
                    $rows_in_batch,
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new ParquetExtractor(
            \is_string($uri) ? Path::realpath($uri) : $uri,
            $options,
            $byte_order,
            $fields,
            $rows_in_batch,
        );
    }

    /**
     * @param Path|string $path
     * @param int $rows_in_group
     * @param null|Schema $schema
     *
     * @throws MissingDependencyException
     *
     * @return Loader
     */
    final public static function to(
        string|Path $path,
        int $rows_in_group = 10000,
        Schema $schema = null,
        ParquetOptions $options = null
    ) : Loader {
        if ($options === null) {
            $options = new ParquetOptions();
            $options->TreatByteArrayAsString = true;
        }

        return new ParquetLoader(
            \is_string($path) ? Path::realpath($path) : $path,
            $rows_in_group,
            $schema,
            $options
        );
    }
}
