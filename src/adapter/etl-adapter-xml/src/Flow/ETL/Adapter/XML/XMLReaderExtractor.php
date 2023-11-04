<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class XMLReaderExtractor implements Extractor, Extractor\FileExtractor
{
    /**
     * In order to iterate only over <element> nodes us root/elements/element.
     *
     * <root>
     *   <elements>
     *     <element></element>
     *     <element></element>
     *   <elements>
     * </root>
     *
     * $xmlNodePath does not support attributes and it's not xpath, it is just a sequence
     * of node names separated with slash.
     *
     * @param string $xmlNodePath
     */
    public function __construct(
        private readonly Path $path,
        private readonly string $xmlNodePath = ''
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            $xmlReader = new \XMLReader();
            $xmlReader->open($filePath->path());

            $previousDepth = 0;
            $currentPathBreadCrumbs = [];

            while ($xmlReader->read()) {
                if ($xmlReader->nodeType === \XMLReader::ELEMENT) {
                    if ($previousDepth === $xmlReader->depth) {
                        \array_pop($currentPathBreadCrumbs);
                        $currentPathBreadCrumbs[] = $xmlReader->name;
                    }

                    if ($xmlReader->depth > $previousDepth) {
                        $currentPathBreadCrumbs[] = $xmlReader->name;
                    }

                    while ($xmlReader->depth < $previousDepth) {
                        \array_pop($currentPathBreadCrumbs);
                        $previousDepth--;
                    }

                    $currentPath = \implode('/', $currentPathBreadCrumbs);

                    if ($currentPath === $this->xmlNodePath || ($this->xmlNodePath === '' && $xmlReader->depth === 0)) {
                        $node = new \DOMDocument('1.0', '');
                        $node->loadXML($xmlReader->readOuterXml());

                        if ($shouldPutInputIntoRows) {
                            $row = Row::create(
                                Entry::xml('node', $node),
                                Entry::string('_input_file_uri', $filePath->uri())
                            );
                        } else {
                            $row = Row::create(Entry::xml('node', $node));
                        }

                        yield new Rows($row);
                    }

                    $previousDepth = $xmlReader->depth;
                }
            }

            $xmlReader->close();
        }

        $context->streams()->close($this->path);
    }

    public function source() : Path
    {
        return $this->path;
    }
}
