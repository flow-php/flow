<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Logger\Logger;
use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Level;
use Monolog\LogRecord;

/**
 * Monolog handler that forwards log records to Flow Telemetry.
 *
 * This handler allows using Monolog as the logging interface while leveraging
 * Telemetry's export capabilities (console, OTLP, etc.).
 *
 * Monolog context and extra data are converted to Telemetry attributes with
 * appropriate prefixes (context.*, extra.*). Throwables in context are handled
 * specially via Telemetry's setException() method.
 *
 * Example usage:
 * ```php
 * use Monolog\Logger as MonologLogger;
 * use function Flow\Bridge\Monolog\Telemetry\DSL\telemetry_handler;
 *
 * $telemetryLogger = $telemetry->logger('my-app');
 * $monolog = new MonologLogger('channel');
 * $monolog->pushHandler(telemetry_handler($telemetryLogger));
 *
 * $monolog->info('User logged in', ['user_id' => 123]);
 * ```
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
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }
}
