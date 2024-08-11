<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Schema;
use Flow\ETL\{Extractor, Loader};
use Flow\Filesystem\Path;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\{ByteOrder, Options};

/**
 * @param array<Path>|Path|string $path
 * @param array<string> $columns
 *
 * @return Extractor
 */
function from_parquet(
    string|Path|array $path,
    array $columns = [],
    Options $options = new Options(),
    ByteOrder $byte_order = ByteOrder::LITTLE_ENDIAN,
    ?int $offset = null,
) : Extractor {
    if (\is_array($path)) {
        $extractors = [];

        if ($offset !== null) {
            throw new InvalidArgumentException('Offset can be used only with single file path, not with pattern');
        }

        foreach ($path as $filePath) {
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
        \is_string($path) ? Path::realpath($path) : $path,
        $options,
        $byte_order,
        $columns,
        $offset
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
