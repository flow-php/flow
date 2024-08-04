<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\{Adapter\XML\Loader\XMLLoader,
    Adapter\XML\XMLWriter\DOMDocumentWriter,
    Extractor
    };
use Flow\Filesystem\Path;

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
    string $root_element_name = 'rows',
    string $row_element_name = 'row',
    XMLWriter $xml_writer = new DOMDocumentWriter()
) : XMLLoader {
    return new XMLLoader(
        \is_string($path) ? Path::realpath($path) : $path,
        $root_element_name,
        $row_element_name,
        $xml_writer
    );
}
