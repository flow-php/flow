<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\Loader;

use Flow\ETL\Adapter\XML\XMLEncoder;
use Flow\ETL\Adapter\XML\XMLOpenSink;
use Flow\ETL\Adapter\XML\XMLWriter;
use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Loader\File\FileLoader;
use Flow\ETL\Loader\File\FileSink;
use Flow\ETL\Loader\File\FileSinks;
use Flow\ETL\Loader\File\FileWriteFrame;
use Flow\ETL\Loader\File\PartitionRouter;
use Flow\ETL\Loader\Partitioning;
use Flow\ETL\Loader\PartitioningLoader;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

use function sprintf;
use function trim;

final class XMLLoader implements Closure, Discardable, FileLoader, FileSinks, Loader, PartitioningLoader
{
    private ?FileWriteFrame $frame = null;

    private PartitionRouter $router;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private string $attributePrefix = '_';

    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = 'Y-m-d\TH:i:s.uP';

    private ?XMLEncoder $encoder = null;

    private string $listElementName = 'element';

    private string $mapElementKeyName = 'key';

    private string $mapElementName = 'element';

    private string $mapElementValueName = 'value';

    private readonly Path $path;

    private string $rootElementName = 'root';

    private string $rowElementName = 'row';

    /**
     * @var array<string, string>
     */
    private array $xmlAttributes = ['version' => '1.0', 'encoding' => 'UTF-8'];

    public function __construct(
        Path $path,
        private readonly XMLWriter $xmlWriter,
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. to_xml($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->router = new PartitionRouter(Partitioning::none());
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::XML);
    }

    public function partitionBy(Partitioning $partitioning): static
    {
        $this->router = new PartitionRouter($partitioning);

        return $this;
    }

    public function closure(FlowContext $context): void
    {
        $this->frame?->closure();
        $this->frame = null;
    }

    public function discard(FlowContext $context): void
    {
        $this->frame?->discard();
        $this->frame = null;
    }

    public function destination(): Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        ($this->frame ??= new FileWriteFrame(
            $this->filesystem,
            $this->path,
            $this->saveMode,
            $this->router,
            $this,
        ))->write($rows, $context, $this);
    }

    public function open(DestinationStream $stream, Backend $backend): FileSink
    {
        $attributes = '';

        foreach ($this->xmlAttributes as $name => $value) {
            $attributes .= $name . '="' . $value . '" ';
        }

        return new XMLOpenSink(
            $stream,
            $this->encoder(),
            '<?xml ' . trim($attributes) . "?>\n<" . $this->rootElementName . ">\n",
            $this->rootElementName,
        );
    }

    public function saveMode(SaveMode $mode): static
    {
        $this->saveMode = $mode;

        return $this;
    }

    public function withAttributePrefix(string $attributePrefix): self
    {
        $this->attributePrefix = $attributePrefix;

        return $this;
    }

    public function withDateFormat(string $dateFormat): self
    {
        $this->dateFormat = $dateFormat;

        return $this;
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $this->dateTimeFormat = $dateTimeFormat;

        return $this;
    }

    public function withListElementName(string $listElementName): self
    {
        $this->listElementName = $listElementName;

        return $this;
    }

    public function withMapElementKeyName(string $mapElementKeyName): self
    {
        $this->mapElementKeyName = $mapElementKeyName;

        return $this;
    }

    public function withMapElementName(string $mapElementName): self
    {
        $this->mapElementName = $mapElementName;

        return $this;
    }

    public function withMapElementValueName(string $mapElementValueName): self
    {
        $this->mapElementValueName = $mapElementValueName;

        return $this;
    }

    public function withRootElementName(string $rootElementName): self
    {
        $this->rootElementName = $rootElementName;

        return $this;
    }

    public function withRowElementName(string $rowElementName): self
    {
        $this->rowElementName = $rowElementName;

        return $this;
    }

    /**
     * @param array<string, string> $xmlAttributes
     */
    public function withXMLAttributes(array $xmlAttributes): self
    {
        $this->xmlAttributes = $xmlAttributes;

        return $this;
    }

    private function encoder(): XMLEncoder
    {
        return $this->encoder ??= new XMLEncoder(
            $this->xmlWriter,
            $this->attributePrefix,
            $this->dateTimeFormat,
            $this->dateFormat,
            $this->listElementName,
            $this->mapElementName,
            $this->mapElementKeyName,
            $this->mapElementValueName,
            $this->rowElementName,
        );
    }
}
