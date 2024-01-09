<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Adapter\XML\Loader\DomDocumentLoader;
use Flow\ETL\Adapter\XML\Loader\XMLWriterLoader;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;

/**
 * @param array<Path|string>|Path|string $path
 */
function from_xml(
    string|Path|array $path,
    string $xml_node_path = ''
) : Extractor {
    if (\is_array($path)) {
        /** @var array<Extractor> $extractors */
        $extractors = [];

        foreach ($path as $next_path) {
            $extractors[] = new XMLReaderExtractor(
                \is_string($next_path) ? Path::realpath($next_path) : $next_path,
                $xml_node_path
            );
        }

        return from_all(...$extractors);
    }

    return new XMLReaderExtractor(
        \is_string($path) ? Path::realpath($path) : $path,
        $xml_node_path
    );
}

function to_xml(
    string|Path $path,
    string $collectionName = 'rows',
    string $collectionElementName = 'row'
) : Loader {
    if (\class_exists(\XMLWriter::class)) {
        return new XMLWriterLoader(\is_string($path) ? Path::realpath($path) : $path, $collectionName, $collectionElementName);
    }

    return new DomDocumentLoader(\is_string($path) ? Path::realpath($path) : $path, $collectionName, $collectionElementName);
}
