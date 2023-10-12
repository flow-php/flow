<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\XML\XMLReaderExtractor;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;

class XML
{
    /**
     * @param array<Path|string>|Path|string $path
     * @param string $xml_node_path
     * @param int $rows_in_batch
     *
     * @return Extractor
     */
    final public static function from(
        string|Path|array $path,
        string $xml_node_path = '',
        int $rows_in_batch = 1000,
    ) : Extractor {
        if (\is_array($path)) {
            /** @var array<Extractor> $extractors */
            $extractors = [];

            foreach ($path as $next_path) {
                $extractors[] = new XMLReaderExtractor(
                    \is_string($next_path) ? Path::realpath($next_path) : $next_path,
                    $xml_node_path,
                    $rows_in_batch,
                );
            }

            return new Extractor\ChainExtractor(...$extractors);
        }

        return new XMLReaderExtractor(
            \is_string($path) ? Path::realpath($path) : $path,
            $xml_node_path,
            $rows_in_batch,
        );
    }
}
