<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Processor;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogSink;
use Flow\Telemetry\Signal\Signals;
use Throwable;

/**
 * Exports each log record immediately when processed.
 *
 * Unlike BatchingLogProcessor, this processor exports log records synchronously
 * one at a time. This is useful for debugging and development where
 * immediate visibility of logs is more important than performance.
 */
final readonly class PassThroughLogProcessor implements LogSink
{
    public function __construct(
        private Exporter $exporter,
        private ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {}

    public function flush(): bool
    {
        return true;
    }

    public function process(LogEntry $entry): void
    {
        try {
            $this->exporter->export(Signals::logs([$entry]));
        } catch (Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }

    public function shutdown(): void
    {
        try {
            $this->exporter->shutdown();
        } catch (Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }
}
