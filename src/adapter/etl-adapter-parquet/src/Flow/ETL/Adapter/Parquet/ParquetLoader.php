<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

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
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Flow\Parquet\Option as ParquetOption;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;

use function sprintf;

final class ParquetLoader implements Closure, Discardable, FileLoader, Loader, PartitioningLoader
{
    private ?FileWriteFrame $frame = null;

    private PartitionRouter $router;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private Compressions $compressions = Compressions::SNAPPY;

    private readonly SchemaConverter $converter;

    private ?ParquetEngine $engine = null;

    private Options $options;

    private readonly Path $path;

    private ?Schema $schema = null;

    public function __construct(Path $path, Filesystem $filesystem = new NativeLocalFilesystem())
    {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. to_parquet($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->router = new PartitionRouter(Partitioning::none());
        $this->converter = new SchemaConverter();
        // every row reaching a loader was validated where it entered the pipeline, so the writer does not validate
        // each value against the column again - withOptions() can still turn it back on
        $this->options = Options::default()->set(ParquetOption::VALIDATE_DATA, false);
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::PARQUET);
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
            new ParquetFileSinks(
                $this->schema?->gracefulRemove(...$this->router->droppedNames()),
                $this->converter,
                $this->compressions,
                $this->options,
                $this->engine,
            ),
        ))->write($rows, $context, $this);
    }

    public function saveMode(SaveMode $mode): static
    {
        $this->saveMode = $mode;

        return $this;
    }

    public function withCompressions(Compressions $compressions): self
    {
        $this->compressions = $compressions;

        return $this;
    }

    public function withEngine(?ParquetEngine $engine): self
    {
        $this->engine = $engine;

        return $this;
    }

    public function withOptions(Options $options): self
    {
        $this->options = $options;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}
