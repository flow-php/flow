<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

/**
 * Log severity levels aligned with OpenTelemetry ranges.
 *
 * Severity indicates the importance and urgency of a log record.
 * These levels align with OpenTelemetry's severity number ranges,
 * enabling proper translation to OTEL protocol.
 *
 * Example usage:
 * ```php
 * $logger->emit(Severity::INFO, 'Processing started');
 *
 * // Check severity level
 * if ($severity->isAtLeast(Severity::WARN)) {
 *     // Handle warning or higher
 * }
 * ```
 */
enum Severity : int
{
    /**
     * Debugging information.
     *
     * Use for diagnostic information useful during development
     * and debugging sessions.
     * OTEL severity range: 5-8
     */
    case DEBUG = 5;

    /**
     * Error messages.
     *
     * Use when an error occurred but the application can continue
     * running.
     * OTEL severity range: 17-20
     */
    case ERROR = 17;

    /**
     * Fatal/critical messages.
     *
     * Use for severe errors that will likely cause the application
     * to terminate or become unusable.
     * OTEL severity range: 21-24
     */
    case FATAL = 21;

    /**
     * Informational messages.
     *
     * Use for general operational information about the application's
     * normal behavior.
     * OTEL severity range: 9-12
     */
    case INFO = 9;
    /**
     * Finest-grained debugging information.
     *
     * Use for very detailed diagnostic information, typically only
     * enabled during development or troubleshooting.
     * OTEL severity range: 1-4
     */
    case TRACE = 1;

    /**
     * Warning messages.
     *
     * Use for potentially harmful situations that don't prevent
     * the application from functioning.
     * OTEL severity range: 13-16
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
    public function isAtLeast(Severity $other) : bool
    {
        return $this->value >= $other->value;
    }

    /**
     * Get the severity name as a string.
     *
     * @return string The severity level name (TRACE, DEBUG, INFO, WARN, ERROR, FATAL)
     */
    public function name() : string
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
