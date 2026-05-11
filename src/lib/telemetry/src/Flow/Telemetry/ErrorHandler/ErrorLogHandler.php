<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * Default ErrorHandler. Writes formatted Throwables via PHP's error_log() —
 * stderr in CLI by default, or the error_log ini setting otherwise.
 *
 * Matches the OTEL spec recommendation to "log to standard error output".
 */
final readonly class ErrorLogHandler implements ErrorHandler
{
    public function __construct(
        private ErrorLogMessageType $messageType = ErrorLogMessageType::OperatingSystem,
        private bool $expandNewlines = false,
        private string $messagePrefix = '[flow-telemetry]',
    ) {}

    public function handle(\Throwable $error): void
    {
        try {
            $message = \sprintf(
                '%s %s: %s in %s:%d',
                $this->messagePrefix,
                $error::class,
                $error->getMessage(),
                $error->getFile(),
                $error->getLine(),
            );

            if ($this->expandNewlines) {
                foreach (\explode("\n", $message) as $line) {
                    \error_log($line, $this->messageType->value);
                }

                return;
            }

            \error_log(\str_replace("\n", ' ', $message), $this->messageType->value);
        } catch (\Throwable) {
        }
    }
}
