<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;

/**
 * @param array<Path>|Path|string $uri
 * @param array<string> $columns
 *
 * @return Extractor
 */
function from_parquet(
    string|Path|array $uri,
    array $columns = [],
    Options $options = new Options(),
    ByteOrder $byte_order = ByteOrder::LITTLE_ENDIAN,
) : Extractor {
    if (\is_array($uri)) {
        $extractors = [];

        foreach ($uri as $filePath) {
            $extractors[] = new ParquetExtractor(
                $filePath,
                $options,
                $byte_order,
                $columns
            );
        }

        return from_all(...$extractors);
    }

    return new ParquetExtractor(
        \is_string($uri) ? Path::realpath($uri) : $uri,
        $options,
        $byte_order,
        $columns
    );
}

/**
 * @param Path|string $path
 * @param null|Schema $schema
 *
 * @return Loader
 */
function to_parquet(
    string|Path $path,
    ?Options $options = null,
    Compressions $compressions = Compressions::SNAPPY,
    ?Schema $schema = null,
) : Loader {
    if ($options === null) {
        $options = Options::default();
    }

    return new ParquetLoader(
        \is_string($path) ? Path::realpath($path) : $path,
        $options,
        $compressions,
        $schema,
    );
}
