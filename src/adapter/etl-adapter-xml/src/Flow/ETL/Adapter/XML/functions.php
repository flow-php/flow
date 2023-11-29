<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;

/**
 * @param array<Path|string>|Path|string $path
 * @param string $xml_node_path
 *
 * @return Extractor
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
