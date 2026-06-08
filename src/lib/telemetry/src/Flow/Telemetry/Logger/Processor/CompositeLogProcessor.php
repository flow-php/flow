<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Processor;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogProcessor;
use Flow\Telemetry\Logger\LogSink;
use Throwable;

/**
 * Forwards log records to multiple processors.
 *
 * This is useful when you need to:
 * - Send logs to multiple backends (e.g., both console and OTLP)
 * - Combine batching with memory storage for testing
 * - Add custom processing alongside export
 */
final readonly class CompositeLogProcessor implements LogSink
{
    /**
     * @param array<LogProcessor> $processors
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
            } catch (Throwable $e) {
                $this->errorHandler->handle($e);
                $success = false;
            }
        }

        return $success;
    }

    public function process(LogEntry $entry): void
    {
        foreach ($this->processors as $processor) {
            try {
                $processor->process($entry);
            } catch (Throwable $e) {
                $this->errorHandler->handle($e);
            }
        }
    }

    /**
     * Get all processors in this composite.
     *
     * @return array<LogProcessor>
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
            } catch (Throwable $e) {
                $this->errorHandler->handle($e);
            }
        }
    }
}
