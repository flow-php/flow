<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Row\Schema;
use Flow\ETL\{Attribute\DocumentationDSL, Attribute\Module, Attribute\Type as DSLType, Loader};
use Flow\Filesystem\Path;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\{ByteOrder, Options};

/**
 * @param array<string> $columns
 */
#[DocumentationDSL(module: Module::PARQUET, type: DSLType::EXTRACTOR)]
function from_parquet(
    string|Path $path,
    array $columns = [],
    Options $options = new Options(),
    ByteOrder $byte_order = ByteOrder::LITTLE_ENDIAN,
    ?int $offset = null,
) : ParquetExtractor {
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
#[DocumentationDSL(module: Module::PARQUET, type: DSLType::LOADER)]
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
