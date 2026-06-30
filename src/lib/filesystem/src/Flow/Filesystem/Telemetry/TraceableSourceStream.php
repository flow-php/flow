<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Flow\Telemetry\Meter\Instrument\Counter;
use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;
use Generator;
use Throwable;

use function strlen;

final class TraceableSourceStream implements SourceStream
{
    private ?Counter $bytesReadCounter = null;

    private ?Meter $meter = null;

    private ?Counter $operationsCounter = null;

    private ?Span $span = null;

    private int $totalBytesRead = 0;

    private ?Tracer $tracer = null;

    public function __construct(
        private readonly SourceStream $stream,
        private readonly FilesystemTelemetryConfig $telemetryConfig,
    ) {
        if ($this->telemetryConfig->options->traceStreams) {
            $this->tracer = $telemetryConfig->telemetry->tracer(
                'flow_php_filesystem',
                PackageVersion::get('flow-php/filesystem'),
            );

            $this->span = $this->tracer->span('Read ' . $this->stream->path()->basename(), SpanKind::INTERNAL, [
                FilesystemTelemetryAttributes::ATTR_STREAM_TYPE => 'source',
                FilesystemTelemetryAttributes::ATTR_PATH_URI => $this->stream->path()->uri(),
                FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL => $this->stream->path()->protocol(),
            ]);
        }

        if ($this->telemetryConfig->options->collectMetrics) {
            $this->meter = $telemetryConfig->telemetry->meter(
                'flow_php_filesystem',
                PackageVersion::get('flow-php/filesystem'),
            );
            $this->bytesReadCounter = $this->meter->createCounter(
                'flow.filesystem.read.size',
                'bytes',
                'Total bytes read from source streams',
            );
            $this->operationsCounter = $this->meter->createCounter(
                'flow.filesystem.read.operations',
                'operations',
                'Number of read operations',
            );
        }
    }

    public function close(): void
    {
        $span = $this->span;

        try {
            $this->stream->close();

            if ($span !== null) {
                $span->setAttribute(FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ, $this->totalBytesRead);
            }
        } catch (Throwable $e) {
            if ($span !== null) {
                $span->setAttribute(FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ, $this->totalBytesRead);
                $span->setAttribute(FilesystemTelemetryAttributes::ATTR_ERROR_TYPE, $e::class);
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

    public function content(): string
    {
        $result = $this->stream->content();
        $bytesRead = strlen($result);
        $this->totalBytesRead += $bytesRead;

        $this->recordMetrics($bytesRead);

        return $result;
    }

    public function isOpen(): bool
    {
        return $this->stream->isOpen();
    }

    /**
     * @param int<1, max> $length
     *
     * @return \Generator<string>
     */
    public function iterate(int $length = 1): Generator
    {
        $bytesReadInOperation = 0;

        foreach ($this->stream->iterate($length) as $chunk) {
            $chunkSize = strlen($chunk);
            $bytesReadInOperation += $chunkSize;
            $this->totalBytesRead += $chunkSize;

            yield $chunk;
        }

        $this->recordMetrics($bytesReadInOperation);
    }

    public function path(): Path
    {
        return $this->stream->path();
    }

    /**
     * @param int<1, max> $length
     */
    public function read(int $length, int $offset): string
    {
        $result = $this->stream->read($length, $offset);
        $bytesRead = strlen($result);
        $this->totalBytesRead += $bytesRead;

        $this->recordMetrics($bytesRead);

        return $result;
    }

    /**
     * @param null|int<1, max> $length
     *
     * @return \Generator<string>
     */
    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        $bytesReadInOperation = 0;

        foreach ($this->stream->readLines($separator, $length) as $line) {
            $lineSize = strlen($line) + strlen($separator);
            $bytesReadInOperation += $lineSize;
            $this->totalBytesRead += $lineSize;

            yield $line;
        }

        $this->recordMetrics($bytesReadInOperation);
    }

    public function size(): ?int
    {
        return $this->stream->size();
    }

    private function recordMetrics(int $bytesRead): void
    {
        if (!$this->telemetryConfig->options->collectMetrics) {
            return;
        }

        $this->bytesReadCounter?->add($bytesRead);
        $this->operationsCounter?->add(1);
    }
}
