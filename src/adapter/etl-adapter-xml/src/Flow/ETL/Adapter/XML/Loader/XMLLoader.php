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
    private string $attributePrefix;

    private string $dateTimeFormat;

    private string $listElementName = 'element';

    private string $mapElementKeyName = 'key';

    private string $mapElementName = 'element';

    private string $mapElementValueName = 'value';

    private string $rootElementName;

    private string $rowElementName;

    /**
     * @var array<string, int>
     */
    private array $writes = [];

    public function __construct(
        private readonly Path $path,
        string $rootElementName,
        string $rowElementName,
        string $attributePrefix,
        string $dateTimeFormat,
        private readonly XMLWriter $xmlWriter
    ) {
        $this->rootElementName = $rootElementName;
        $this->rowElementName = $rowElementName;
        $this->attributePrefix = $attributePrefix;
        $this->dateTimeFormat = $dateTimeFormat;
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
        $normalizer = new RowsNormalizer(
            new EntryNormalizer(
                new PHPValueNormalizer(
                    $context->config->caster(),
                    $this->attributePrefix,
                    $this->dateTimeFormat,
                    $this->listElementName,
                    $this->mapElementName,
                    $this->mapElementKeyName,
                    $this->mapElementValueName
                ),
            ),
            $this->rowElementName
        );

        $this->write($rows, $rows->partitions()->toArray(), $context, $normalizer);
    }

    public function withAttributePrefix(string $attributePrefix) : self
    {
        $this->attributePrefix = $attributePrefix;

        return $this;
    }

    public function withDateTimeFormat(string $dateTimeFormat) : self
    {
        $this->dateTimeFormat = $dateTimeFormat;

        return $this;
    }

    public function withListElementName(string $listElementName) : self
    {
        $this->listElementName = $listElementName;

        return $this;
    }

    public function withMapElementKeyName(string $mapElementKeyName) : self
    {
        $this->mapElementKeyName = $mapElementKeyName;

        return $this;
    }

    public function withMapElementName(string $mapElementName) : self
    {
        $this->mapElementName = $mapElementName;

        return $this;
    }

    public function withMapElementValueName(string $mapElementValueName) : self
    {
        $this->mapElementValueName = $mapElementValueName;

        return $this;
    }

    public function withRootElementName(string $rootElementName) : self
    {
        $this->rootElementName = $rootElementName;

        return $this;
    }

    public function withRowElementName(string $rowElementName) : self
    {
        $this->rowElementName = $rowElementName;

        return $this;
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
