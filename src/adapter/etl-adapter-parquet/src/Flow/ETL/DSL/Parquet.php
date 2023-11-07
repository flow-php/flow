<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\ETL\Adapter\Parquet\ParquetLoader;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;
use Flow\ETL\Row\Schema;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;

/**
 * @infection-ignore-all
 */
class Parquet
{
    /**
     * @param array<Path>|Path|string $uri
     * @param array<string> $columns
     *
     * @return Extractor
     */
    final public static function from(
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

            return new Extractor\ChainExtractor(...$extractors);
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
    final public static function to(
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
}
