<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

enum Severity: int
{
    /**
     * Diagnostic information for development; OTEL severity 5-8.
     */
    case DEBUG = 5;

    /**
     * An error the application survives; OTEL severity 17-20.
     */
    case ERROR = 17;

    /**
     * An error that will likely stop the application; OTEL severity 21-24.
     */
    case FATAL = 21;

    /**
     * Normal operational information; OTEL severity 9-12.
     */
    case INFO = 9;
    /**
     * Finest-grained diagnostics; OTEL severity 1-4.
     */
    case TRACE = 1;

    /**
     * A potentially harmful situation that does not stop the application; OTEL severity 13-16.
     */
    case WARN = 13;

    /**
     * Check if this severity is at least as severe as another.
     *
     * This is useful for filtering logs by minimum severity level.
     *
     * @param Severity $other The severity to compare against
     *
     * @return bool True if this severity is greater than or equal to $other
     */
    public function isAtLeast(Severity $other): bool
    {
        return $this->value >= $other->value;
    }

    /**
     * Get the severity name as a string.
     *
     * @return string The severity level name (TRACE, DEBUG, INFO, WARN, ERROR, FATAL)
     */
    public function name(): string
    {
        return match ($this) {
            self::TRACE => 'TRACE',
            self::DEBUG => 'DEBUG',
            self::INFO => 'INFO',
            self::WARN => 'WARN',
            self::ERROR => 'ERROR',
            self::FATAL => 'FATAL',
        };
    }
}
