<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Processor;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanProcessor;

/**
 * Forwards spans to multiple processors.
 *
 * This is useful when you need to:
 * - Send spans to multiple backends (e.g., both console and OTLP)
 * - Combine batching with memory storage for testing
 * - Add custom processing alongside export
 */
final readonly class CompositeSpanProcessor implements SpanProcessor
{
    /**
     * @param array<SpanProcessor> $processors
     */
    public function __construct(
        private array $processors,
        private ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {}

    public function flush(): bool
    {
        $success = true;

        foreach ($this->processors as $processor) {
            try {
                if (!$processor->flush()) {
                    $success = false;
                }
            } catch (\Throwable $e) {
                $this->errorHandler->handle($e);
                $success = false;
            }
        }

        return $success;
    }

    public function onEnd(Span $span): void
    {
        foreach ($this->processors as $processor) {
            try {
                $processor->onEnd($span);
            } catch (\Throwable $e) {
                $this->errorHandler->handle($e);
            }
        }
    }

    public function onStart(Span $span): void
    {
        foreach ($this->processors as $processor) {
            try {
                $processor->onStart($span);
            } catch (\Throwable $e) {
                $this->errorHandler->handle($e);
            }
        }
    }

    /**
     * Get all processors in this composite.
     *
     * @return array<SpanProcessor>
     */
    public function processors(): array
    {
        return $this->processors;
    }

    public function shutdown(): void
    {
        foreach ($this->processors as $processor) {
            try {
                $processor->shutdown();
            } catch (\Throwable $e) {
                $this->errorHandler->handle($e);
            }
        }
    }
}
