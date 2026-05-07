<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Processor;

use Flow\Telemetry\ErrorHandler\{ErrorHandler, ErrorLogHandler};
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\{LogEntry, LogProcessor};
use Flow\Telemetry\Signal\Signals;

/**
 * Batches log records for efficient export.
 *
 * Collects log records in memory and exports them in batches when:
 * - The batch size limit is reached
 * - flush() is explicitly called
 * - the system is shutting down
 */
final class BatchingLogProcessor implements LogProcessor
{
    /**
     * @var array<LogEntry>
     */
    private array $buffer = [];

    private bool $isShutdown = false;

    public function __construct(
        private readonly Exporter $exporter,
        private readonly int $batchSize = 512,
        private readonly ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {
    }

    public function flush() : bool
    {
        if (\count($this->buffer) === 0) {
            return true;
        }

        $entries = $this->buffer;
        $this->buffer = [];

        try {
            return $this->exporter->export(Signals::logs($entries));
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);

            return false;
        }
    }

    public function process(LogEntry $entry) : void
    {
        $this->buffer[] = $entry;

        if (\count($this->buffer) >= $this->batchSize) {
            $this->flush();
        }
    }

    public function shutdown() : void
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
