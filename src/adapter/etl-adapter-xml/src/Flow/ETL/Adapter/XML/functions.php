<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use function Flow\Filesystem\DSL\path_real;
use Flow\ETL\{Adapter\XML\Loader\XMLLoader,
    Adapter\XML\XMLWriter\DOMDocumentWriter,
    Attribute\DocumentationDSL,
    Attribute\DocumentationExample,
    Attribute\Module,
    Attribute\Type as DSLType};
use Flow\Filesystem\Path;

/**
 *  In order to iterate only over <element> nodes use `from_xml($file)->withXMLNodePath('root/elements/element')`.
 *
 *  <root>
 *    <elements>
 *      <element></element>
 *      <element></element>
 *    <elements>
 *  </root>
 *
 *  XML Node Path does not support attributes and it's not xpath, it is just a sequence
 *  of node names separated with slash.
 *
 * @param Path|string $path
 * @param string $xml_node_path - @deprecated use `from_xml($file)->withXMLNodePath($xmlNodePath)` method instead
 */
#[DocumentationDSL(module: Module::XML, type: DSLType::EXTRACTOR)]
#[DocumentationExample(topic: 'data_reading', example: 'xml')]
function from_xml(
    Path|string $path,
    string $xml_node_path = '',
) : XMLParserExtractor {
    return (new XMLParserExtractor(\is_string($path) ? path_real($path) : $path))->withXMLNodePath($xml_node_path);
}

/**
 * @param Path|string $path
 * @param string $root_element_name - @deprecated use `withRootElementName()` method instead
 * @param string $row_element_name - @deprecated use `withRowElementName()` method instead
 * @param string $attribute_prefix - @deprecated use `withAttributePrefix()` method instead
 * @param string $date_time_format - @deprecated use `withDateTimeFormat()` method instead
 * @param XMLWriter $xml_writer
 */
#[DocumentationDSL(module: Module::XML, type: DSLType::LOADER)]
function to_xml(
    string|Path $path,
    string $root_element_name = 'rows',
    string $row_element_name = 'row',
    string $attribute_prefix = '_',
    string $date_time_format = 'Y-m-d\TH:i:s.uP',
    XMLWriter $xml_writer = new DOMDocumentWriter(),
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
