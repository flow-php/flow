<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Logger\Logger;
use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Level;
use Monolog\LogRecord;
use Throwable;

/**
 * Context and extra become `context.*`/`extra.*` attributes; a Throwable goes through setException().
 */
final class TelemetryHandler extends AbstractProcessingHandler
{
    /**
     * @param Logger $logger The Flow Telemetry logger to forward logs to
     * @param LogRecordConverter $converter Converter to transform Monolog LogRecord to Telemetry LogRecord
     * @param Level $level The minimum logging level at which this handler will be triggered
     * @param bool $bubble Whether messages handled by this handler should bubble up to other handlers
     */
    public function __construct(
        private readonly Logger $logger,
        private readonly LogRecordConverter $converter = new LogRecordConverter(),
        Level $level = Level::Debug,
        bool $bubble = true,
        private readonly ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {
        parent::__construct($level, $bubble);
    }

    protected function write(LogRecord $record): void
    {
        try {
            $this->logger->emit($this->converter->convert($record));
        } catch (Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }
}
