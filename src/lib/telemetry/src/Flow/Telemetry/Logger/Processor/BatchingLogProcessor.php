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

use function count;
use function hrtime;

/**
 * Batches log records for efficient export.
 *
 * Collects log records in memory and exports them in batches when:
 * - The batch size limit is reached
 * - The max batch age elapses since the first buffered record (when configured)
 * - flush() is explicitly called
 * - the system is shutting down
 */
final class BatchingLogProcessor implements LogSink
{
    /**
     * @var array<LogEntry>
     */
    private array $buffer = [];

    private bool $isShutdown = false;

    private ?int $batchStartedAt = null;

    public function __construct(
        private readonly Exporter $exporter,
        private readonly int $batchSize = 512,
        private readonly ErrorHandler $errorHandler = new ErrorLogHandler(),
        private readonly ?float $maxBatchAgeSeconds = null,
    ) {}

    public function flush(): bool
    {
        if (count($this->buffer) === 0) {
            return true;
        }

        $entries = $this->buffer;
        $this->buffer = [];
        $this->batchStartedAt = null;

        try {
            return $this->exporter->export(Signals::logs($entries));
        } catch (Throwable $e) {
            $this->errorHandler->handle($e);

            return false;
        }
    }

    public function process(LogEntry $entry): void
    {
        if (count($this->buffer) === 0) {
            $this->batchStartedAt = (int) hrtime(true);
        }

        $this->buffer[] = $entry;

        if (count($this->buffer) >= $this->batchSize || $this->isBatchExpired()) {
            $this->flush();
        }
    }

    public function shutdown(): void
    {
        if ($this->isShutdown) {
            return;
        }

        $this->isShutdown = true;

        $this->flush();

        try {
            $this->exporter->shutdown();
        } catch (Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }

    private function isBatchExpired(): bool
    {
        if ($this->maxBatchAgeSeconds === null || $this->batchStartedAt === null) {
            return false;
        }

        return (((int) hrtime(true) - $this->batchStartedAt) / 1_000_000_000) >= $this->maxBatchAgeSeconds;
    }
}
