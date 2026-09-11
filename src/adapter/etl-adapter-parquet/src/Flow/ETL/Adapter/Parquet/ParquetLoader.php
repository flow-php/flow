<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\FilesSink;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Loader\Partitioning;
use Flow\ETL\Loader\PartitioningLoader;
use Flow\ETL\Loader\PartitionRouter;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Flow\Parquet\Engine\AdaptiveParquetEngine;
use Flow\Parquet\Option as ParquetOption;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Writer;
use Throwable;

use function sprintf;

final class ParquetLoader implements Closure, Discardable, FileLoader, Loader, PartitioningLoader
{
    private PartitionRouter $router;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private ?FilesSink $files = null;

    /** @var array<string, Writer> */
    private array $writers = [];

    private Compressions $compressions = Compressions::SNAPPY;

    private readonly SchemaConverter $converter;

    private ?ParquetEncoder $encoder = null;

    private ?ParquetEngine $engine = null;

    private ?Schema $inferredSchema = null;

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
        $this->closeWriters();

        $this->files?->publish();
        $this->files = null;
    }

    public function discard(FlowContext $context): void
    {
        $this->closeWriters();

        $this->files?->abandon();
        $this->files = null;
    }

    public function destination(): Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $context->telemetry()->loadingStarted($this, [
            TelemetryAttributes::ATTR_LOADER_DESTINATION_URI => $this->path->uri(),
        ]);

        try {
            if ($this->schema === null && $this->inferredSchema === null) {
                $this->inferSchema($rows);
            }

            foreach ($this->router->route($rows) as [$partitions, $group]) {
                $stream = ($this->files ??= new FilesSink($this->filesystem, $this->path, $this->saveMode))->writeTo(
                    $partitions->toArray(),
                );

                ($this->writers[$stream->path()->uri()] ??=
                    $this->openWriter($stream))->writeBatch($this->encoder()->encode(
                    $context->hydrator()->dehydrate($group),
                ));
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
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

    private function closeWriters(): void
    {
        foreach ($this->writers as $uri => $writer) {
            unset($this->writers[$uri]);
            $writer->close();
        }
    }

    private function encoder(): ParquetEncoder
    {
        return $this->encoder ??= new ParquetEncoder($this->converter->toParquet($this->schema()));
    }

    private function inferSchema(Rows $rows): void
    {
        if ($this->inferredSchema === null) {
            $this->inferredSchema = $rows->schema()->makeNullable();
        } else {
            $this->inferredSchema = $this->inferredSchema->merge($rows->schema())->makeNullable();
        }
    }

    private function openWriter(DestinationStream $stream): Writer
    {
        $writer = new Writer(
            compression: $this->compressions,
            options: $this->options,
            engine: $this->engine ?? new AdaptiveParquetEngine(),
        );
        $writer->openForStream($stream, $this->converter->toParquet($this->schema()));

        return $writer;
    }

    private function schema(): Schema
    {
        return (
            $this->schema ?? $this->inferredSchema ?? throw new RuntimeException(
                'Schema has not been inferred yet. Load at least one batch of rows first.',
            )
        );
    }
}
