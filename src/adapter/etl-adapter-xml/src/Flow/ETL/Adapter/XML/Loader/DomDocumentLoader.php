<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Loader;

use Flow\ETL\Adapter\XML\RowsNormalizer;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Rows;

final class DomDocumentLoader implements Closure, Loader, Loader\FileLoader
{
    public function __construct(
        private readonly Path $path,
        private readonly string $collectionName = 'rows',
        private readonly string $collectionElementName = 'row',
        private readonly RowsNormalizer $normalizer = new RowsNormalizer(),
    ) {
        if ($this->path->isPattern()) {
            throw new \InvalidArgumentException("XMLLoader path can't be pattern, given: " . $this->path->path());
        }
    }

    public function closure(FlowContext $context) : void
    {
        foreach ($context->streams() as $stream) {
            if ($stream->path()->extension() === 'xml') {
                \fwrite($stream->resource(), "</{$this->collectionName}>");
            }
        }

        $context->streams()->close($this->path);
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        $streams = $context->streams();

        if (!$streams->isOpen($this->path, $rows->partitions()->toArray())) {
            $stream = $streams->open($this->path, 'xml', $context->appendSafe(), $rows->partitions()->toArray());

            \fwrite($stream->resource(), (new \DOMDocument('1.0', 'utf-8'))->saveXML() . "<{$this->collectionName}>");
        } else {
            $stream = $streams->open($this->path, 'xml', $context->appendSafe(), $rows->partitions()->toArray());
        }

        $this->writeXML($rows, $stream);
    }

    /**
     * @throws RuntimeException
     * @throws \DOMException
     */
    private function writeXML(Rows $rows, FileStream $stream) : void
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

            \fwrite($stream->resource(), $dom->saveXML($dom->documentElement) ?: '');
        }
    }
}
