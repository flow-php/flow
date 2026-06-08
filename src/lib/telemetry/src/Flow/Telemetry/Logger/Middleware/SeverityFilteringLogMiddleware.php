<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Middleware;

use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogMiddleware;
use Flow\Telemetry\Logger\Severity;

/**
 * Drops log entries below a minimum severity level.
 *
 * Returns the entry unchanged when its severity is at or above the threshold,
 * or null to drop it (short-circuiting the rest of the pipeline).
 */
final readonly class SeverityFilteringLogMiddleware implements LogMiddleware
{
    public function __construct(
        private Severity $minimumSeverity = Severity::INFO,
    ) {}

    public function process(LogEntry $entry): ?LogEntry
    {
        if ($entry->record->severity->isAtLeast($this->minimumSeverity)) {
            return $entry;
        }

        return null;
    }
}
