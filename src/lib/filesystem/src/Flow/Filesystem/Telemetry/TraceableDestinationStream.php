<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

use Flow\Filesystem\{DestinationStream, Path};
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus, Tracer};

final readonly class TraceableDestinationStream implements DestinationStream
{
    private Tracer $tracer;

    public function __construct(
        private DestinationStream $stream,
        private FilesystemTelemetryConfig $telemetryConfig,
    ) {
        $this->tracer = $telemetryConfig->telemetry->tracer(
            'flow.filesystem',
            PackageVersion::get('flow-php/filesystem'),
        );
    }

    public function append(string $data) : DestinationStream
    {
        if (!$this->telemetryConfig->options->traceStreamOperations) {
            $this->stream->append($data);

            return $this;
        }

        $span = $this->tracer->span(
            'DestinationStream::append',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_STREAM_TYPE => 'destination',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $this->stream->path()->uri(),
                FilesystemTelemetryAttributes::ATTR_BYTES_WRITTEN => \strlen($data),
            ]
        );

        try {
            $this->stream->append($data);
            $span->setStatus(SpanStatus::ok());

            return $this;
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function close() : void
    {
        if (!$this->telemetryConfig->options->traceStreamOperations) {
            $this->stream->close();

            return;
        }

        $span = $this->tracer->span(
            'DestinationStream::close',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_STREAM_TYPE => 'destination',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $this->stream->path()->uri(),
            ]
        );

        try {
            $this->stream->close();
            $span->setStatus(SpanStatus::ok());
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    /**
     * @param resource $resource
     */
    public function fromResource($resource) : DestinationStream
    {
        if (!$this->telemetryConfig->options->traceStreamOperations) {
            $this->stream->fromResource($resource);

            return $this;
        }

        $span = $this->tracer->span(
            'DestinationStream::fromResource',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_STREAM_TYPE => 'destination',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $this->stream->path()->uri(),
            ]
        );

        try {
            $this->stream->fromResource($resource);
            $span->setStatus(SpanStatus::ok());

            return $this;
        } catch (\Throwable $e) {
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function isOpen() : bool
    {
        return $this->stream->isOpen();
    }

    public function path() : Path
    {
        return $this->stream->path();
    }
}
