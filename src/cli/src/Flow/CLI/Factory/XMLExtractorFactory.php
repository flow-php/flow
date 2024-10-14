<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\ETL\Adapter\XML\from_xml;
use Flow\CLI\Options\TypedOption;
use Flow\ETL\Adapter\XML\XMLParserExtractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final class XMLExtractorFactory
{
    public function __construct(
        private readonly Path $path,
        private readonly string $nodePathOption = 'xml-node-path',
        private readonly string $bufferSizeOption = 'xml-buffer-size',
    ) {
    }

    public function get(InputInterface $input) : XMLParserExtractor
    {
        $extractor = from_xml($this->path);

        $nodePath = (new TypedOption($this->nodePathOption))->asStringNullable($input);
        $bufferSize = (new TypedOption($this->bufferSizeOption))->asIntNullable($input);

        if ($nodePath !== null) {
            $extractor->withXMLNodePath($nodePath);
        }

        if ($bufferSize !== null && $bufferSize > 0) {
            $extractor->withBufferSize($bufferSize);
        }

        return $extractor;
    }
}
