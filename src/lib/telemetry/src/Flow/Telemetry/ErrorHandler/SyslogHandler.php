<?php

declare(strict_types=1);

namespace Flow\Telemetry\ErrorHandler;

/**
 * Writes formatted Throwables to the OS syslog facility via openlog/syslog/closelog.
 */
final readonly class SyslogHandler implements ErrorHandler
{
    public function __construct(
        private string $ident = 'flow-telemetry',
        private SyslogFacility $facility = SyslogFacility::User,
        private int $logOpts = \LOG_PID,
        private SyslogSeverity $severity = SyslogSeverity::Error,
    ) {
        if ($ident === '') {
            throw new \InvalidArgumentException('SyslogHandler ident must be a non-empty string');
        }
    }

    public function handle(\Throwable $error): void
    {
        try {
            \openlog($this->ident, $this->logOpts, $this->facility->value);
            \syslog(
                $this->severity->value,
                \sprintf('%s: %s in %s:%d', $error::class, $error->getMessage(), $error->getFile(), $error->getLine()),
            );
            \closelog();
        } catch (\Throwable) {
        }
    }
}
