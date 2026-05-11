<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Processor;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanProcessor;

/**
 * Batches spans for efficient export.
 *
 * Collects spans in memory and exports them in batches when:
 * - The batch size limit is reached
 * - flush() is explicitly called
 * - the system is shutting down
 */
final class BatchingSpanProcessor implements SpanProcessor
{
    /**
     * @var array<Span>
     */
    private array $buffer = [];

    private bool $isShutdown = false;

    public function __construct(
        private readonly Exporter $exporter,
        private readonly int $batchSize = 512,
        private readonly ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {}

    public function flush(): bool
    {
        if (\count($this->buffer) === 0) {
            return true;
        }

        $spans = $this->buffer;
        $this->buffer = [];

        try {
            return $this->exporter->export(Signals::traces($spans));
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);

            return false;
        }
    }

    public function onEnd(Span $span): void
    {
        $this->buffer[] = $span;

        if (\count($this->buffer) >= $this->batchSize) {
            $this->flush();
        }
    }

    public function onStart(Span $span): void {}

    public function shutdown(): void
    {
        if ($this->isShutdown) {
            return;
        }

        $this->isShutdown = true;

        $this->flush();

        try {
            $this->exporter->shutdown();
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }
}
