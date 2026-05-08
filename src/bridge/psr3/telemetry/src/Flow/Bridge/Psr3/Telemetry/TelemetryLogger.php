<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry;

use Flow\Bridge\Psr3\Telemetry\Exception\InvalidArgumentException;
use Flow\Telemetry\ErrorHandler\{ErrorHandler, ErrorLogHandler};
use Flow\Telemetry\Logger\Logger;
use Psr\Log\{AbstractLogger, InvalidArgumentException as PsrInvalidArgumentException};

final class TelemetryLogger extends AbstractLogger
{
    public function __construct(
        private readonly Logger $logger,
        private readonly LogRecordConverter $converter = new LogRecordConverter(),
        private readonly ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {
    }

    /**
     * @param array<array-key, mixed> $context
     */
    public function log($level, string|\Stringable $message, array $context = []) : void
    {
        if (!\is_string($level) && !$level instanceof \Stringable) {
            throw new InvalidArgumentException('PSR-3 log level must be a string or Stringable.');
        }

        try {
            $this->logger->emit($this->converter->convert($level, $message, $context));
        } catch (PsrInvalidArgumentException $e) {
            throw $e;
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }
}
