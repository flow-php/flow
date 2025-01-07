<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\CLI\option_string_nullable;
use function Flow\ETL\Adapter\XML\to_xml;
use Flow\ETL\Adapter\XML\Loader\XMLLoader;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class XMLLoaderFactory
{
    public function __construct(
        private Path $path,
        private string $rootElementName = 'output-xml-root-element',
        private string $rowElementName = 'output-xml-row-element',
        private string $attributePrefix = 'output-xml-attribute-prefix',
        private string $dateTimeFormat = 'output-xml-date-time-format',
    ) {
    }

    public function get(InputInterface $input) : XMLLoader
    {
        $extractor = to_xml($this->path);

        $rootElementName = option_string_nullable($this->rootElementName, $input);
        $rowElementName = option_string_nullable($this->rowElementName, $input);
        $attributePrefix = option_string_nullable($this->attributePrefix, $input);
        $dateTimeFormat = option_string_nullable($this->dateTimeFormat, $input);

        if ($rootElementName !== null) {
            $extractor->withRootElementName($rootElementName);
        }

        if ($rowElementName !== null) {
            $extractor->withRowElementName($rowElementName);
        }

        if ($attributePrefix !== null) {
            $extractor->withAttributePrefix($attributePrefix);
        }

        if ($dateTimeFormat !== null) {
            $extractor->withDateTimeFormat($dateTimeFormat);
        }

        return $extractor;
    }
}
