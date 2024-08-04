<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Loader;

use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer\PHPValueNormalizer;
use Flow\ETL\Adapter\XML\{RowsNormalizer, XMLWriter};
use Flow\ETL\Loader\Closure;
use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\Filesystem\{DestinationStream, Partition, Path};

final class XMLLoader implements Closure, Loader, Loader\FileLoader
{
    /**
     * @var array<string, int>
     */
    private array $writes = [];

    public function __construct(
        private readonly Path $path,
        private readonly string $rootElementName,
        private readonly string $rowElementName,
        private readonly XMLWriter $xmlWriter
    ) {
    }

    public function closure(FlowContext $context) : void
    {
        foreach ($context->streams() as $stream) {
            if ($stream->path()->extension() === 'xml') {
                $stream->append('</' . $this->rootElementName . '>');
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
        $normalizer = new RowsNormalizer(new EntryNormalizer(new PHPValueNormalizer($context->config->caster())), $this->rowElementName);

        $this->write($rows, $rows->partitions()->toArray(), $context, $normalizer);
    }

    /**
     * @param array<Partition> $partitions
     */
    public function write(Rows $nextRows, array $partitions, FlowContext $context, RowsNormalizer $normalizer) : void
    {
        $streams = $context->streams();

        if (!$streams->isOpen($this->path, $partitions)) {
            $stream = $streams->writeTo($this->path, $partitions);

            if (!\array_key_exists($stream->path()->path(), $this->writes)) {
                $this->writes[$stream->path()->path()] = 0;
            }

            $stream->append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<" . $this->rootElementName . ">\n");
        } else {
            $stream = $streams->writeTo($this->path, $partitions);
        }

        $this->writeXML($nextRows, $stream, $normalizer);
    }

    /**
     * @param Rows $rows
     * @param DestinationStream $stream
     */
    public function writeXML(Rows $rows, DestinationStream $stream, RowsNormalizer $normalizer) : void
    {
        if (!\count($rows)) {
            return;
        }

        foreach ($normalizer->normalize($rows) as $node) {
            $stream->append($this->xmlWriter->write($node) . "\n");
        }

        $this->writes[$stream->path()->path()]++;
    }
}
