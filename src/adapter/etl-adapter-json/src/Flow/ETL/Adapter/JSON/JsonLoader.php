<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use DateTimeInterface;
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

final class JsonLoader implements Closure, Discardable, FileLoader, FileSinks, Loader, PartitioningLoader
{
    private ?FileWriteFrame $frame = null;

    private PartitionRouter $router;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = DateTimeInterface::ATOM;

    private int $flags = JSON_THROW_ON_ERROR;

    private readonly Path $path;

    public function __construct(
        Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        private JsonFraming $framing = JsonFraming::Array,
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. %s($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
                $framing === JsonFraming::Lines ? 'to_json_lines' : 'to_json',
            ));
        }

        $this->filesystem = $filesystem;
        $this->router = new PartitionRouter(Partitioning::none());
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::JSON);
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
        return new JsonOpenSink(
            $stream,
            new AdaptiveJsonEncoder($this->flags, $this->dateTimeFormat, $this->dateFormat),
            $this->framing,
        );
    }

    public function saveMode(SaveMode $mode): static
    {
        $this->saveMode = $mode;

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

    public function withFlags(int $flags): self
    {
        $this->flags = $this->framing === JsonFraming::Lines ? $flags & ~JSON_PRETTY_PRINT : $flags;

        return $this;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function withRowsInNewLines(bool $putRowsInNewLines): self
    {
        if ($this->framing === JsonFraming::Lines) {
            throw new InvalidArgumentException('withRowsInNewLines() applies to to_json(), not to_json_lines()');
        }

        $this->framing = $putRowsInNewLines ? JsonFraming::ArrayLines : JsonFraming::Array;

        return $this;
    }
}
