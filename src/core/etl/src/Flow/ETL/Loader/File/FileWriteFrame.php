<?php

declare(strict_types=1);

namespace Flow\ETL\Loader\File;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Throwable;

final class FileWriteFrame
{
    private ?FilesSink $files = null;

    /**
     * @var array<string, FileSink> by stream URI
     */
    private array $open = [];

    public function __construct(
        private readonly Filesystem $filesystem,
        private readonly Path $path,
        private readonly SaveMode $saveMode,
        private readonly PartitionRouter $router,
        private readonly FileSinks $sinks,
    ) {}

    /**
     * Closes every sink - the first close failure rethrown once all were closed - then publishes the files.
     *
     * @throws Throwable
     */
    public function closure(): void
    {
        $failure = $this->closeSinks();

        if ($failure !== null) {
            throw $failure;
        }

        $this->files?->publish();
        $this->files = null;
    }

    /**
     * Closes every sink, then abandons the files whatever a close threw.
     *
     * @throws Throwable
     */
    public function discard(): void
    {
        $failure = $this->closeSinks();

        $this->files?->abandon();
        $this->files = null;

        if ($failure !== null) {
            throw $failure;
        }
    }

    public function write(Rows $rows, FlowContext $context, Loader $loader): void
    {
        $context->telemetry()->loadingStarted($loader, [
            TelemetryAttributes::ATTR_LOADER_DESTINATION_URI => $this->path->uri(),
        ]);

        try {
            foreach ($this->router->route($rows) as [$partitions, $group]) {
                $files = $this->files ??= new FilesSink($this->filesystem, $this->path, $this->saveMode);
                $stream = $files->writeTo($partitions->toArray());

                ($this->open[$stream->path()->uri()] ??= $this->sinks->open($stream, $context->backend()))->write(
                    $group,
                );
            }

            $context->telemetry()->loadingCompleted($loader, [
                TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count(),
            ]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($loader, $e);

            throw $e;
        }
    }

    /**
     * Closes every open sink, all of them even when one throws.
     *
     * @return null|Throwable the first close failure
     */
    private function closeSinks(): ?Throwable
    {
        $failure = null;

        foreach ($this->open as $uri => $sink) {
            unset($this->open[$uri]);

            try {
                $sink->close();
            } catch (Throwable $e) {
                $failure ??= $e;
            }
        }

        return $failure;
    }
}
