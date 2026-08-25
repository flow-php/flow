<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Output;

use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\ETL\Adapter\XML\XMLWriter;
use Flow\ETL\Adapter\XML\XMLWriter\DOMDocumentWriter;
use Flow\ETL\Loader;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use RuntimeException;

use function Flow\ETL\Adapter\XML\to_xml;

if (!function_exists('Flow\ETL\Adapter\XML\to_xml')) {
    throw new RuntimeException(
        'Flow\ETL\Adapter\XML\to_xml function is not available. Make sure that composer require flow-php/etl-adapter-xml dependency is present in your composer.json.',
    );
}

final readonly class XMLOutput implements Output
{
    public function __construct(
        private string $rootElementName = 'rows',
        private string $rowElementName = 'row',
        private string $attributePrefix = '_',
        private string $dateTimeFormat = 'Y-m-d\TH:i:s.uP',
        private XMLWriter $xmlWriter = new DOMDocumentWriter(),
    ) {}

    public function loader(Path $path, Filesystem $filesystem): Loader
    {
        return to_xml($path, xml_writer: $this->xmlWriter, filesystem: $filesystem)
            ->withRootElementName($this->rootElementName)
            ->withRowElementName($this->rowElementName)
            ->withAttributePrefix($this->attributePrefix)
            ->withDateTimeFormat($this->dateTimeFormat);
    }

    public function type(): Type
    {
        return Type::XML;
    }
}
