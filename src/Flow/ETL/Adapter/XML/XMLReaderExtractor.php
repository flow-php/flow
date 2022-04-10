<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\ETL\Rows;

/**
 * @psalm-immutable
 */
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
        private readonly string $xmlFilePath,
        private readonly string $xmlNodePath,
        private readonly int $rowsInBatch,
        private readonly string $rowEntryName = 'row'
    ) {
        if (!\strlen($xmlNodePath)) {
            throw new InvalidArgumentException('XML Node Path cant be empty');
        }
    }

    /**
     * @psalm-suppress ImpureMethodCall
     */
    public function extract() : \Generator
    {
        $xmlReader = new \XMLReader();
        $xmlReader->open($this->xmlFilePath);

        $previousDepth = 0;
        $currentPathBreadCrumbs = [];

        $rows = new Rows();

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

                if ($currentPath === $this->xmlNodePath) {
                    $rows = $rows->add(Row::create(XMLEntry::fromString($this->rowEntryName, $xmlReader->readOuterXml())));

                    if ($rows->count() >= $this->rowsInBatch) {
                        yield $rows;
                        $rows = new Rows();
                    }
                }
                $previousDepth = $xmlReader->depth;
            }
        }

        $xmlReader->close();

        if ($rows->count()) {
            yield $rows;
        }
    }
}
