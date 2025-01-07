<?php

declare(strict_types=1);

namespace Flow\CLI\Factory;

use function Flow\CLI\{option_int_nullable, option_string_nullable};
use function Flow\ETL\Adapter\XML\from_xml;
use Flow\ETL\Adapter\XML\XMLParserExtractor;
use Flow\Filesystem\Path;
use Symfony\Component\Console\Input\InputInterface;

final readonly class XMLExtractorFactory
{
    public function __construct(
        private Path $path,
        private string $nodePathOption = 'input-xml-node-path',
        private string $bufferSizeOption = 'input-xml-buffer-size',
    ) {
    }

    public function get(InputInterface $input) : XMLParserExtractor
    {
        $extractor = from_xml($this->path);

        $nodePath = option_string_nullable($this->nodePathOption, $input);
        $bufferSize = option_int_nullable($this->bufferSizeOption, $input);

        if ($nodePath !== null) {
            $extractor->withXMLNodePath($nodePath);
        }

        if ($bufferSize !== null && $bufferSize > 0) {
            $extractor->withBufferSize($bufferSize);
        }

        return $extractor;
    }
}
