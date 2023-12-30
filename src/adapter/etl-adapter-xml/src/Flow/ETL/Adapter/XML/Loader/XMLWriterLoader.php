<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Loader;

use Flow\ETL\Adapter\XML\RowsNormalizer;
use Flow\ETL\Loader\Closure;
use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\Filesystem\{Partition, Path};

final class XMLWriterLoader implements Closure, Loader, Loader\FileLoader
{
    private readonly RowsNormalizer $normalizer;

    /**
     * @var array<string, \XMLWriter>
     */
    private array $writers = [];

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
                $this->writers[$stream->path()->path()]->endDocument();
                $this->writers[$stream->path()->path()]->flush();
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
        $this->write($rows, $rows->partitions()->toArray(), $context);
    }

    /**
     * @param array<Partition> $partitions
     */
    private function write(Rows $rows, array $partitions, FlowContext $context) : void
    {
        $streams = $context->streams();

        $stream = $streams->writeTo($this->path, $partitions);

        if (!\array_key_exists($stream->path()->path(), $this->writers)) {
            $writer = new \XMLWriter();
            $writer->openUri($stream->path()->path());
            $writer->startDocument('1.0', 'UTF-8');
            $writer->startElement($this->collectionName);

            $this->writers[$stream->path()->path()] = $writer;
        } else {
            $writer = $this->writers[$stream->path()->path()];
        }

        foreach ($this->normalizer->normalize($rows) as $row) {
            $writer->startElement($this->collectionElementName);

            foreach ($row as $name => $value) {
                $writer->writeElement($name, $value);
            }

            $writer->endElement();
        }

        $writer->flush();
    }
}
