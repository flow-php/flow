<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Output;

use function Flow\ETL\Adapter\XML\to_xml;
use function Flow\Filesystem\DSL\path_stdout;
use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\ETL\Adapter\XML\XMLWriter;
use Flow\ETL\Adapter\XML\XMLWriter\DOMDocumentWriter;
use Flow\ETL\Loader;

if (!function_exists('Flow\ETL\Adapter\XML\to_xml')) {
    throw new \RuntimeException('Flow\ETL\Adapter\XML\to_xml function is not available. Make sure that composer require flow-php/etl-adapter-xml dependency is present in your composer.json.');
}

final readonly class XMLOutput implements Output
{
    public function __construct(
        private string $rootElementName = 'rows',
        private string $rowElementName = 'row',
        private string $attributePrefix = '_',
        private string $dateTimeFormat = 'Y-m-d\TH:i:s.uP',
        private XMLWriter $xmlWriter = new DOMDocumentWriter(),
    ) {
    }

    public function loader() : Loader
    {
        return to_xml(path_stdout(['stream' => 'output']), xml_writer: $this->xmlWriter)
            ->withRootElementName($this->rootElementName)
            ->withRowElementName($this->rowElementName)
            ->withAttributePrefix($this->attributePrefix)
            ->withDateTimeFormat($this->dateTimeFormat);
    }

    public function type() : Type
    {
        return Type::XML;
    }
}
