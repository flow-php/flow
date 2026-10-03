<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Loader\File\FileLoader;
use Flow\ETL\Loader\File\FileWriteFrame;
use Flow\ETL\Loader\File\PartitionRouter;
use Flow\ETL\Loader\Partitioning;
use Flow\ETL\Loader\PartitioningLoader;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function sprintf;

final class FloeLoader implements Closure, Discardable, FileLoader, Loader, PartitioningLoader
{
    private ?FileWriteFrame $frame = null;

    private PartitionRouter $router;

    public function partitionBy(Partitioning $partitioning): static
    {
        $this->router = new PartitionRouter($partitioning);

        return $this;
    }

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private ?Schema $schema = null;

    public function __construct(
        private readonly Path $path,
        private readonly ?Metadata $metadata = null,
        private readonly Options $options = new Options(),
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. to_floe($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->router = new PartitionRouter(Partitioning::none());
    }

    public function saveMode(SaveMode $mode): static
    {
        $this->saveMode = $mode;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

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
        // partitionBy() keeps the partition columns in the path rather than the body, so the file schema must not
        // declare them either. Every other declared column stays - a batch that does not carry one is the writer's
        // to refuse.
        ($this->frame ??= new FileWriteFrame(
            $this->filesystem,
            $this->path,
            $this->saveMode,
            $this->router,
            new FloeFileSinks(
                $this->filesystem,
                $this->schema?->gracefulRemove(...$this->router->droppedNames()),
                $this->metadata,
                $this->options,
            ),
        ))->write($rows, $context, $this);
    }
}
