<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

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

final class TextLoader implements Closure, Discardable, FileLoader, FileSinks, Loader, PartitioningLoader
{
    private ?FileWriteFrame $frame = null;

    private PartitionRouter $router;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private ?TextEncoder $encoder = null;

    private string $newLineSeparator = PHP_EOL;

    private readonly Path $path;

    public function __construct(Path $path, Filesystem $filesystem = new NativeLocalFilesystem())
    {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. to_text($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->router = new PartitionRouter(Partitioning::none());
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE->value, ContentType::TEXT);
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
        return new TextOpenSink($stream, $this->encoder());
    }

    public function saveMode(SaveMode $mode): static
    {
        $this->saveMode = $mode;

        return $this;
    }

    public function withNewLineSeparator(string $newLineSeparator): self
    {
        $this->newLineSeparator = $newLineSeparator;

        return $this;
    }

    private function encoder(): TextEncoder
    {
        return $this->encoder ??= new TextEncoder($this->newLineSeparator);
    }
}
