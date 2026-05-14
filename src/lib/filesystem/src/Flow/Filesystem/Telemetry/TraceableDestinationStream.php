<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Path;
use Flow\Telemetry\Meter\Instrument\Counter;
use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;

final class TraceableDestinationStream implements DestinationStream
{
    private ?Counter $bytesWrittenCounter = null;

    private ?Meter $meter = null;

    private ?Counter $operationsCounter = null;

    private ?Span $span = null;

    private int $totalBytesWritten = 0;

    private ?Tracer $tracer = null;

    public function __construct(
        private readonly DestinationStream $stream,
        private readonly FilesystemTelemetryConfig $telemetryConfig,
    ) {
        if ($this->telemetryConfig->options->traceStreams) {
            $this->tracer = $telemetryConfig->telemetry->tracer(
                'flow_php_filesystem',
                PackageVersion::get('flow-php/filesystem'),
            );

            $this->span = $this->tracer->span('Write ' . $this->stream->path()->basename(), SpanKind::INTERNAL, [
                FilesystemTelemetryAttributes::ATTR_STREAM_TYPE => 'destination',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $this->stream->path()->uri(),
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL => $this->stream->path()->protocol(),
            ]);
        }

        if ($this->telemetryConfig->options->collectMetrics) {
            $this->meter = $telemetryConfig->telemetry->meter(
                'flow_php_filesystem',
                PackageVersion::get('flow-php/filesystem'),
            );
            $this->bytesWrittenCounter = $this->meter->createCounter(
                'write_size',
                'bytes',
                'Total bytes written to destination streams',
            );
            $this->operationsCounter = $this->meter->createCounter(
                'write_operations',
                'operations',
                'Number of write operations',
            );
        }
    }

    public function append(string $data): DestinationStream
    {
        $bytesWritten = \strlen($data);

        $this->stream->append($data);
        $this->totalBytesWritten += $bytesWritten;

        $this->recordMetrics($bytesWritten);

        return $this;
    }

    public function close(): void
    {
        $span = $this->span;

        try {
            $this->stream->close();

            if ($span !== null) {
                $span->setAttribute(FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN, $this->totalBytesWritten);
                $span->setStatus(SpanStatus::ok());
            }
        } catch (\Throwable $e) {
            if ($span !== null) {
                $span->setAttribute(FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN, $this->totalBytesWritten);
                $span->recordException($e, $this->telemetryConfig->clock->now());
                $span->setStatus(SpanStatus::error($e->getMessage()));
            }

            throw $e;
        } finally {
            if ($span !== null && $this->tracer !== null) {
                $this->tracer->complete($span);
                $this->span = null;
            }
        }
    }

    /**
     * @param resource $resource
     */
    public function fromResource($resource): DestinationStream
    {
        $startPos = \ftell($resource);

        $this->stream->fromResource($resource);

        $endPos = \ftell($resource);
        $bytesWritten = $startPos !== false && $endPos !== false ? $endPos - $startPos : 0;
        $this->totalBytesWritten += $bytesWritten;

        $this->recordMetrics($bytesWritten);

        return $this;
    }

    public function isOpen(): bool
    {
        return $this->stream->isOpen();
    }

    public function path(): Path
    {
        return $this->stream->path();
    }

    private function recordMetrics(int $bytesWritten): void
    {
        if (!$this->telemetryConfig->options->collectMetrics) {
            return;
        }

        $this->bytesWrittenCounter?->add($bytesWritten);
        $this->operationsCounter?->add(1);
    }
}
