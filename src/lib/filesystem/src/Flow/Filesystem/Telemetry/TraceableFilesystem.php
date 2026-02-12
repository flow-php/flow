<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

use Flow\Filesystem\{DestinationStream, FileStatus, Filesystem, Path, Protocol, SourceStream};
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\PackageVersion;

final readonly class TraceableFilesystem implements Filesystem
{
    private Logger $logger;

    public function __construct(
        private Filesystem $filesystem,
        private FilesystemTelemetryConfig $telemetryConfig,
    ) {
        $this->logger = $telemetryConfig->telemetry->logger(
            'flow.filesystem',
            PackageVersion::get('flow-php/filesystem'),
        );
    }

    public function appendTo(Path $path) : DestinationStream
    {
        $stream = $this->filesystem->appendTo($path);

        if (!$this->telemetryConfig->options->traceStreams
            && !$this->telemetryConfig->options->collectMetrics) {
            return $stream;
        }

        return new TraceableDestinationStream($stream, $this->telemetryConfig);
    }

    public function getSystemTmpDir() : Path
    {
        return $this->filesystem->getSystemTmpDir();
    }

    /**
     * @return \Generator<FileStatus>
     */
    public function list(Path $path, Filter $pathFilter = new KeepAll()) : \Generator
    {
        $this->logOperation('list', $path);

        yield from $this->filesystem->list($path, $pathFilter);
    }

    public function mv(Path $from, Path $to) : bool
    {
        $this->logOperation('mv', $from, $to);

        return $this->filesystem->mv($from, $to);
    }

    public function protocol() : Protocol
    {
        return $this->filesystem->protocol();
    }

    public function readFrom(Path $path) : SourceStream
    {
        $stream = $this->filesystem->readFrom($path);

        if (!$this->telemetryConfig->options->traceStreams
            && !$this->telemetryConfig->options->collectMetrics) {
            return $stream;
        }

        return new TraceableSourceStream($stream, $this->telemetryConfig);
    }

    public function rm(Path $path) : bool
    {
        $this->logOperation('rm', $path);

        return $this->filesystem->rm($path);
    }

    public function status(Path $path) : ?FileStatus
    {
        $this->logOperation('status', $path);

        return $this->filesystem->status($path);
    }

    public function writeTo(Path $path) : DestinationStream
    {
        $stream = $this->filesystem->writeTo($path);

        if (!$this->telemetryConfig->options->traceStreams
            && !$this->telemetryConfig->options->collectMetrics) {
            return $stream;
        }

        return new TraceableDestinationStream($stream, $this->telemetryConfig);
    }

    private function logOperation(string $operation, Path $path, ?Path $toPath = null) : void
    {
        $attributes = [
            FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL => $this->filesystem->protocol()->name,
            FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION => $operation,
            FilesystemTelemetryAttributes::ATTR_PATH_URI => $path->uri(),
        ];

        if ($toPath !== null) {
            $attributes[FilesystemTelemetryAttributes::ATTR_PATH_TO] = $toPath->uri();
        }

        $this->logger->debug('Filesystem operation: ' . $operation, $attributes);
    }
}
