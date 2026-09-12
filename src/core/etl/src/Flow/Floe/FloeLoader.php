<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
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
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Exception\FloeException;
use Throwable;

use function sprintf;

final class FloeLoader implements Closure, Discardable, FileLoader, Loader, PartitioningLoader
{
    private PartitionRouter $router;

    public function partitionBy(Partitioning $partitioning): static
    {
        $this->router = new PartitionRouter($partitioning);

        return $this;
    }

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private ?FilesSink $files = null;

    /** @var array<string, FloeWriter> */
    private array $writers = [];

    private ?Schema $schema = null;

    private ?Schema $inferredSchema = null;

    public function __construct(
        private readonly Path $path,
        private readonly ?Metadata $metadata = null,
        private readonly Options $options = new Options(),
        private readonly FloeEngine $engine = FloeEngine::adaptive,
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
        $this->closeWriters();

        $this->files?->publish();
        $this->files = null;
    }

    public function discard(FlowContext $context): void
    {
        try {
            $this->closeWriters();
        } finally {
            $this->files?->abandon();
            $this->files = null;
        }
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
                $this->inferredSchema = $rows->schema();
            }

            foreach ($this->router->route($rows) as [$partitions, $group]) {
                $stream = ($this->files ??= new FilesSink($this->filesystem, $this->path, $this->saveMode))->writeTo(
                    $partitions->toArray(),
                );

                ($this->writers[$stream->path()->uri()] ??= $this->openWriter($stream, $context))->write($group);
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    private function closeWriters(): void
    {
        $failure = null;

        foreach ($this->writers as $uri => $writer) {
            unset($this->writers[$uri]);

            try {
                $writer->close();
            } catch (Throwable $closeFailure) {
                $failure ??= $closeFailure;
            }
        }

        if ($failure !== null) {
            throw $failure;
        }
    }

    private function openWriter(DestinationStream $stream, FlowContext $context): FloeWriter
    {
        $declared =
            $this->schema ?? $this->inferredSchema ?? throw new FloeException(
                'Floe loader has no schema to write with',
            );

        // partitionBy() keeps the partition columns in the path rather than the body, so the file
        // schema must not declare them either. Every other declared column stays - a batch that does
        // not carry one is the writer's to refuse.

        $writer = new FloeWriter(
            $this->filesystem,
            $declared->gracefulRemove(...$this->router->droppedNames()),
            $this->options,
            hydrator: $context->hydrator(),
            engine: $this->engine,
        );
        $writer->createForStream($stream, $this->metadata);

        return $writer;
    }
}
