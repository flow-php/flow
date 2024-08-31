<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use function Flow\ETL\DSL\from_all;
use Flow\ETL\{Adapter\XML\Loader\XMLLoader,
    Adapter\XML\XMLWriter\DOMDocumentWriter,
    Attribute\DocumentationDSL,
    Attribute\Module,
    Attribute\Type as DSLType,
    Extractor};
use Flow\Filesystem\Path;

/**
 * @param array<Path|string>|Path|string $path
 */
#[DocumentationDSL(module: Module::XML, type: DSLType::EXTRACTOR)]
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

/**
 * @param Path|string $path
 * @param string $root_element_name - @deprecated use `withRootElementName()` method instead
 * @param string $row_element_name - @deprecated use `withRowElementName()` method instead
 * @param string $attribute_prefix - @deprecated use `withAttributePrefix()` method instead
 * @param string $date_time_format - @deprecated use `withDateTimeFormat()` method instead
 * @param DOMDocumentWriter $xml_writer
 */
#[DocumentationDSL(module: Module::XML, type: DSLType::LOADER)]
function to_xml(
    string|Path $path,
    string $root_element_name = 'rows',
    string $row_element_name = 'row',
    string $attribute_prefix = '_',
    string $date_time_format = 'Y-m-d\TH:i:s.uP',
    XMLWriter $xml_writer = new DOMDocumentWriter()
) : XMLLoader {
    return (new XMLLoader(
        \is_string($path) ? Path::realpath($path) : $path,
        $xml_writer
    ))
        ->withRootElementName($root_element_name)
        ->withRowElementName($row_element_name)
        ->withAttributePrefix($attribute_prefix)
        ->withDateTimeFormat($date_time_format);
}
