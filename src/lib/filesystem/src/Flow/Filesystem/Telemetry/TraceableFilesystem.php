<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

use Flow\Filesystem\{DestinationStream, FileStatus, Filesystem, Path, Protocol, SourceStream};
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus, Tracer};

final readonly class TraceableFilesystem implements Filesystem
{
    private Tracer $tracer;

    public function __construct(
        private Filesystem $filesystem,
        private FilesystemTelemetryConfig $telemetryConfig,
    ) {
        $this->tracer = $telemetryConfig->telemetry->tracer(
            'flow.filesystem',
            PackageVersion::get('flow-php/filesystem'),
        );
    }

    public function appendTo(Path $path) : DestinationStream
    {
        if (!$this->telemetryConfig->options->traceFilesystemOperations) {
            return $this->wrapDestinationStream($this->filesystem->appendTo($path), $path);
        }

        $span = $this->tracer->span(
            'Filesystem::appendTo',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL => $this->filesystem->protocol()->name,
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION => 'appendTo',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $path->uri(),
            ]
        );

        try {
            $stream = $this->filesystem->appendTo($path);
            $span->setStatus(SpanStatus::ok());

            return $this->wrapDestinationStream($stream, $path);
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
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
        if (!$this->telemetryConfig->options->traceFilesystemOperations) {
            yield from $this->filesystem->list($path, $pathFilter);

            return;
        }

        $span = $this->tracer->span(
            'Filesystem::list',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL => $this->filesystem->protocol()->name,
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION => 'list',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $path->uri(),
                FilesystemTelemetryAttributes::ATTR_PATH_IS_PATTERN => $path->isPattern(),
            ]
        );

        try {
            yield from $this->filesystem->list($path, $pathFilter);
            $span->setStatus(SpanStatus::ok());
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function mv(Path $from, Path $to) : bool
    {
        if (!$this->telemetryConfig->options->traceFilesystemOperations) {
            return $this->filesystem->mv($from, $to);
        }

        $span = $this->tracer->span(
            'Filesystem::mv',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL => $this->filesystem->protocol()->name,
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION => 'mv',
                FilesystemTelemetryAttributes::ATTR_PATH_FROM => $from->uri(),
                FilesystemTelemetryAttributes::ATTR_PATH_TO => $to->uri(),
            ]
        );

        try {
            $result = $this->filesystem->mv($from, $to);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function protocol() : Protocol
    {
        return $this->filesystem->protocol();
    }

    public function readFrom(Path $path) : SourceStream
    {
        if (!$this->telemetryConfig->options->traceFilesystemOperations) {
            return $this->wrapSourceStream($this->filesystem->readFrom($path), $path);
        }

        $span = $this->tracer->span(
            'Filesystem::readFrom',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL => $this->filesystem->protocol()->name,
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION => 'readFrom',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $path->uri(),
            ]
        );

        try {
            $stream = $this->filesystem->readFrom($path);
            $span->setStatus(SpanStatus::ok());

            return $this->wrapSourceStream($stream, $path);
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function rm(Path $path) : bool
    {
        if (!$this->telemetryConfig->options->traceFilesystemOperations) {
            return $this->filesystem->rm($path);
        }

        $span = $this->tracer->span(
            'Filesystem::rm',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL => $this->filesystem->protocol()->name,
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION => 'rm',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $path->uri(),
            ]
        );

        try {
            $result = $this->filesystem->rm($path);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function status(Path $path) : ?FileStatus
    {
        if (!$this->telemetryConfig->options->traceFilesystemOperations) {
            return $this->filesystem->status($path);
        }

        $span = $this->tracer->span(
            'Filesystem::status',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL => $this->filesystem->protocol()->name,
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION => 'status',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $path->uri(),
            ]
        );

        try {
            $result = $this->filesystem->status($path);
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function writeTo(Path $path) : DestinationStream
    {
        if (!$this->telemetryConfig->options->traceFilesystemOperations) {
            return $this->wrapDestinationStream($this->filesystem->writeTo($path), $path);
        }

        $span = $this->tracer->span(
            'Filesystem::writeTo',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL => $this->filesystem->protocol()->name,
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION => 'writeTo',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $path->uri(),
            ]
        );

        try {
            $stream = $this->filesystem->writeTo($path);
            $span->setStatus(SpanStatus::ok());

            return $this->wrapDestinationStream($stream, $path);
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    private function wrapDestinationStream(DestinationStream $stream, Path $path) : DestinationStream
    {
        if (!$this->telemetryConfig->options->traceStreamOperations) {
            return $stream;
        }

        return new TraceableDestinationStream($stream, $this->telemetryConfig);
    }

    private function wrapSourceStream(SourceStream $stream, Path $path) : SourceStream
    {
        if (!$this->telemetryConfig->options->traceStreamOperations) {
            return $stream;
        }

        return new TraceableSourceStream($stream, $this->telemetryConfig);
    }
}
