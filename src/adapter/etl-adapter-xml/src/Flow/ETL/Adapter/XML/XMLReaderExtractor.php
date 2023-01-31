<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class XMLReaderExtractor implements Extractor
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
        private readonly string $xmlNodePath = '',
        private readonly int $rowsInBatch = 1000,
        private readonly string $rowEntryName = 'row'
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            $xmlReader = new \XMLReader();
            $xmlReader->open($filePath->path());

            $previousDepth = 0;
            $currentPathBreadCrumbs = [];

            $rows = [];

            while ($xmlReader->read()) {
                if ($xmlReader->nodeType === \XMLReader::ELEMENT) {
                    if ($previousDepth === $xmlReader->depth) {
                        \array_pop($currentPathBreadCrumbs);
                        \array_push($currentPathBreadCrumbs, $xmlReader->name);
                    }

                    if ($xmlReader->depth > $previousDepth) {
                        \array_push($currentPathBreadCrumbs, $xmlReader->name);
                    }

                    if ($xmlReader->depth < $previousDepth) {
                        \array_pop($currentPathBreadCrumbs);
                    }

                    $currentPath = \implode('/', $currentPathBreadCrumbs);

                    if ($currentPath === $this->xmlNodePath || ($this->xmlNodePath === '' && $xmlReader->depth === 0)) {
                        $node = new \DOMDocument('1.0', '');
                        /** @psalm-suppress ArgumentTypeCoercion */
                        $node->loadXML($xmlReader->readOuterXml());

                        if ($context->config->shouldPutInputIntoRows()) {
                            $rows[] = Row::create(
                                Entry::array($this->rowEntryName, $this->convertDOMDocument($node)),
                                Entry::string('input_file_uri', $filePath->uri())
                            );
                        } else {
                            $rows[] = Row::create(Entry::array($this->rowEntryName, $this->convertDOMDocument($node)));
                        }

                        if (\count($rows) >= $this->rowsInBatch) {
                            yield new Rows(...$rows);
                            $rows = [];
                        }
                    }

                    $previousDepth = $xmlReader->depth;
                }
            }

            $xmlReader->close();

            if (\count($rows)) {
                yield new Rows(...$rows);
            }
        }
    }

    /**
     * @param \DOMDocument $document
     *
     * @return array<mixed>
     */
    private function convertDOMDocument(\DOMDocument $document) : array
    {
        $xmlArray = [];

        /** @psalm-suppress ImpureMethodCall */
        if ($document->hasChildNodes()) {
            $children = $document->childNodes;

            foreach ($children as $child) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $xmlArray[$child->nodeName] = $this->convertDOMElement($child);
            }
        }

        return $xmlArray;
    }

    /**
     * @psalm-suppress ArgumentTypeCoercion
     * @psalm-suppress PossiblyNullArgument
     * @psalm-suppress UnnecessaryVarAnnotation
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress PossiblyNullIterator
     *
     * @return array<mixed>
     */
    private function convertDOMElement(\DOMElement|\DOMNode $element) : array
    {
        $xmlArray = [];

        if ($element->hasAttributes()) {
            /**
             * @var \DOMAttr $attribute
             *
             * @phpstan-ignore-next-line
             */
            foreach ($element->attributes as $attribute) {
                $xmlArray['@attributes'][$attribute->name] = $attribute->value;
            }
        }

        foreach ($element->childNodes as $childNode) {
            if ($childNode->nodeType === XML_TEXT_NODE) {
                /** @phpstan-ignore-next-line  */
                if (\trim($childNode->nodeValue)) {
                    $xmlArray['@value'] = $childNode->nodeValue;
                }
            }

            if ($childNode->nodeType === XML_ELEMENT_NODE) {
                if ($this->isElementCollection($element)) {
                    /** @phpstan-ignore-next-line */
                    $xmlArray[$childNode->nodeName][] = $this->convertDOMElement($childNode);
                } else {
                    $xmlArray[$childNode->nodeName] = $this->convertDOMElement($childNode);
                }
            }
        }

        return $xmlArray;
    }

    /**
     * @psalm-suppress ImpureMethodCall
     */
    private function isElementCollection(\DOMElement|\DOMNode $element) : bool
    {
        if ($element->childNodes->count() <= 1) {
            return false;
        }

        $nodeNames = [];
        /** @var \DOMElement $childNode */
        foreach ($element->childNodes as $childNode) {
            if ($childNode->nodeType === XML_ELEMENT_NODE) {
                $nodeNames[] = $childNode->nodeName;
            }
        }

        if (!\count($nodeNames) || \count($nodeNames) === 1) {
            return false;
        }

        return \count(\array_unique($nodeNames)) === 1;
    }
}
