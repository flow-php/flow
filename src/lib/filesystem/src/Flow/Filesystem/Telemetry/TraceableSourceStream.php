<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

use Flow\Filesystem\{Path, SourceStream};
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Tracer\{SpanKind, SpanStatus, Tracer};

final readonly class TraceableSourceStream implements SourceStream
{
    private Tracer $tracer;

    public function __construct(
        private SourceStream $stream,
        private FilesystemTelemetryConfig $telemetryConfig,
    ) {
        $this->tracer = $telemetryConfig->telemetry->tracer(
            'flow.filesystem',
            PackageVersion::get('flow-php/filesystem'),
        );
    }

    public function close() : void
    {
        if (!$this->telemetryConfig->options->traceStreamOperations) {
            $this->stream->close();

            return;
        }

        $span = $this->tracer->span(
            'SourceStream::close',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_STREAM_TYPE => 'source',
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

    public function content() : string
    {
        if (!$this->telemetryConfig->options->traceStreamOperations) {
            return $this->stream->content();
        }

        $span = $this->tracer->span(
            'SourceStream::content',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_STREAM_TYPE => 'source',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $this->stream->path()->uri(),
            ]
        );

        try {
            $result = $this->stream->content();
            $span->setAttribute(FilesystemTelemetryAttributes::ATTR_BYTES_READ, \strlen($result));
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

    public function isOpen() : bool
    {
        return $this->stream->isOpen();
    }

    /**
     * @param int<1, max> $length
     *
     * @return \Generator<string>
     */
    public function iterate(int $length = 1) : \Generator
    {
        if (!$this->telemetryConfig->options->traceStreamOperations) {
            yield from $this->stream->iterate($length);

            return;
        }

        $span = $this->tracer->span(
            'SourceStream::iterate',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_STREAM_TYPE => 'source',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $this->stream->path()->uri(),
            ]
        );

        $bytesRead = 0;

        try {
            foreach ($this->stream->iterate($length) as $chunk) {
                $bytesRead += \strlen($chunk);

                yield $chunk;
            }

            $span->setAttribute(FilesystemTelemetryAttributes::ATTR_BYTES_READ, $bytesRead);
            $span->setStatus(SpanStatus::ok());
        } catch (\Throwable $e) {
            $span->setAttribute(FilesystemTelemetryAttributes::ATTR_BYTES_READ, $bytesRead);
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function path() : Path
    {
        return $this->stream->path();
    }

    /**
     * @param int<1, max> $length
     */
    public function read(int $length, int $offset) : string
    {
        if (!$this->telemetryConfig->options->traceStreamOperations) {
            return $this->stream->read($length, $offset);
        }

        $span = $this->tracer->span(
            'SourceStream::read',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_STREAM_TYPE => 'source',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $this->stream->path()->uri(),
            ]
        );

        try {
            $result = $this->stream->read($length, $offset);
            $span->setAttribute(FilesystemTelemetryAttributes::ATTR_BYTES_READ, \strlen($result));
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

    /**
     * @param null|int<1, max> $length
     *
     * @return \Generator<string>
     */
    public function readLines(string $separator = "\n", ?int $length = null) : \Generator
    {
        if (!$this->telemetryConfig->options->traceStreamOperations) {
            yield from $this->stream->readLines($separator, $length);

            return;
        }

        $span = $this->tracer->span(
            'SourceStream::readLines',
            SpanKind::INTERNAL,
            [
                FilesystemTelemetryAttributes::ATTR_STREAM_TYPE => 'source',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $this->stream->path()->uri(),
            ]
        );

        $bytesRead = 0;

        try {
            foreach ($this->stream->readLines($separator, $length) as $line) {
                $bytesRead += \strlen($line) + \strlen($separator);

                yield $line;
            }

            $span->setAttribute(FilesystemTelemetryAttributes::ATTR_BYTES_READ, $bytesRead);
            $span->setStatus(SpanStatus::ok());
        } catch (\Throwable $e) {
            $span->setAttribute(FilesystemTelemetryAttributes::ATTR_BYTES_READ, $bytesRead);
            $span->recordException($e, new \DateTimeImmutable());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $this->tracer->complete($span);
        }
    }

    public function size() : ?int
    {
        return $this->stream->size();
    }
}
