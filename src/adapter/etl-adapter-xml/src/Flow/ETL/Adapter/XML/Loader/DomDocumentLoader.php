<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Loader;

use Flow\ETL\Adapter\XML\RowsNormalizer;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\Closure;
use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\Filesystem\{DestinationStream, Path};

final class DomDocumentLoader implements Closure, Loader, Loader\FileLoader
{
    private readonly RowsNormalizer $normalizer;

    public function __construct(
        private readonly Path $path,
        private readonly string $collectionName = 'rows',
        private readonly string $collectionElementName = 'row',
    ) {
        if ($this->path->isPattern()) {
            throw new \InvalidArgumentException("XMLLoader path can't be pattern, given: " . $this->path->path());
        }

        $this->normalizer = new RowsNormalizer();
    }

    public function closure(FlowContext $context) : void
    {
        foreach ($context->streams() as $stream) {
            if ($stream->path()->extension() === 'xml') {
                $stream->append("</{$this->collectionName}>");
            }
        }

        $context->streams()->closeWriters($this->path);
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $streams = $context->streams();

        if (!$streams->isOpen($this->path, $rows->partitions()->toArray())) {
            $stream = $streams->writeTo($this->path, $rows->partitions()->toArray());

            $stream->append((new \DOMDocument('1.0', 'utf-8'))->saveXML() . "<{$this->collectionName}>");
        } else {
            $stream = $streams->writeTo($this->path, $rows->partitions()->toArray());
        }

        $this->writeXML($rows, $stream);
    }

    /**
     * @throws RuntimeException
     * @throws \DOMException
     */
    private function writeXML(Rows $rows, DestinationStream $stream) : void
    {
        foreach ($this->normalizer->normalize($rows) as $row) {
            $dom = new \DOMDocument('1.0', 'utf-8');

            $rowElement = $dom->createElement($this->collectionElementName);

            foreach ($row as $name => $value) {
                $rowItem = $dom->createElement($name);
                $rowItem->appendChild($dom->createTextNode($value));

                $rowElement->appendChild($rowItem);
            }

            $dom->appendChild($rowElement);

            $stream->append($dom->saveXML($dom->documentElement) ?: '');
        }
    }
}
