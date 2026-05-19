<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Processor;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanProcessor;
use Throwable;

/**
 * Exports each span immediately when it ends.
 *
 * Unlike BatchingSpanProcessor, this processor exports spans synchronously
 * one at a time. This is useful for debugging and development where
 * immediate visibility of spans is more important than performance.
 */
final readonly class PassThroughSpanProcessor implements SpanProcessor
{
    public function __construct(
        private Exporter $exporter,
        private ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {}

    public function flush(): bool
    {
        return true;
    }

    public function onEnd(Span $span): void
    {
        try {
            $this->exporter->export(Signals::traces([$span]));
        } catch (Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }

    public function onStart(Span $span): void {}

    public function shutdown(): void
    {
        try {
            $this->exporter->shutdown();
        } catch (Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }
}
