<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\{Adapter\XML\Loader\XMLLoader,
    Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer,
    Adapter\XML\XMLWriter\DOMDocumentWriter,
    Attribute\DSL,
    Attribute\Module,
    Attribute\Type as DSLType,
    Extractor};
use Flow\Filesystem\Path;

/**
 * @param array<Path|string>|Path|string $path
 */
#[DSL(module: Module::XML, type: DSLType::EXTRACTOR)]
function from_xml(
    string|Path|array $path,
    string $xml_node_path = ''
) : Extractor {
    if (\is_array($path)) {
        /** @var array<Extractor> $extractors */
        $extractors = [];

        foreach ($path as $next_path) {
            $extractors[] = new XMLParserExtractor(
                \is_string($next_path) ? Path::realpath($next_path) : $next_path,
                $xml_node_path
            );
        }

        return from_all(...$extractors);
    }

    return new XMLParserExtractor(
        \is_string($path) ? Path::realpath($path) : $path,
        $xml_node_path
    );
}

#[DSL(module: Module::XML, type: DSLType::LOADER)]
function to_xml(
    string|Path $path,
    string $root_element_name = 'rows',
    string $row_element_name = 'row',
    string $attribute_prefix = '_',
    string $date_time_format = PHPValueNormalizer::DATE_TIME_FORMAT,
    XMLWriter $xml_writer = new DOMDocumentWriter()
) : XMLLoader {
    return new XMLLoader(
        \is_string($path) ? Path::realpath($path) : $path,
        $root_element_name,
        $row_element_name,
        $attribute_prefix,
        $date_time_format,
        $xml_writer
    );
}
