<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry;

use Flow\Bridge\Monolog\Telemetry\Exception\InvalidArgumentException;
use Flow\Telemetry\Logger\Severity;
use Monolog\Level;

/**
 * Maps Monolog log levels to Flow Telemetry severity levels.
 *
 * Provides a configurable mapping from Monolog's 8 levels to Telemetry's 6 severity levels.
 * By default:
 * - DEBUG → DEBUG
 * - INFO, NOTICE → INFO
 * - WARNING → WARN
 * - ERROR → ERROR
 * - CRITICAL, ALERT, EMERGENCY → FATAL
 *
 * Custom mappings can be provided to override the defaults.
 *
 * Example with custom mapping:
 * ```php
 * $mapper = new SeverityMapper([
 *     Level::Notice->value => Severity::WARN,  // Override: NOTICE → WARN instead of INFO
 * ]);
 * ```
 */
final readonly class SeverityMapper
{
    /**
     * @var array<int, Severity>
     */
    private array $mapping;

    /**
     * @param null|array<int, Severity> $customMapping Optional custom mapping (Monolog Level value => Telemetry Severity)
     */
    public function __construct(?array $customMapping = null)
    {
        $this->mapping = $customMapping ?? self::defaultMapping();
    }

    /**
     * Get the default mapping from Monolog levels to Telemetry severity.
     *
     * @return array<int, Severity>
     */
    public static function defaultMapping(): array
    {
        return [
            Level::Debug->value => Severity::DEBUG,
            Level::Info->value => Severity::INFO,
            Level::Notice->value => Severity::INFO,
            Level::Warning->value => Severity::WARN,
            Level::Error->value => Severity::ERROR,
            Level::Critical->value => Severity::FATAL,
            Level::Alert->value => Severity::FATAL,
            Level::Emergency->value => Severity::FATAL,
        ];
    }

    /**
     * Map a Monolog Level to a Telemetry Severity.
     *
     * @param Level $level The Monolog log level
     *
     * @throws InvalidArgumentException When no mapping exists for the given level
     *
     * @return Severity The corresponding Telemetry severity
     */
    public function map(Level $level): Severity
    {
        if (!isset($this->mapping[$level->value])) {
            throw new InvalidArgumentException("No mapping defined for Monolog level: {$level->name}");
        }

        return $this->mapping[$level->value];
    }
}
