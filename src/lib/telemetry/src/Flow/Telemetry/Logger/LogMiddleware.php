<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

/**
 * Stateless; return a new entry to enrich, null to drop.
 */
interface LogMiddleware
{
    /**
     * Transform or filter a log entry.
     *
     * Return the entry to pass to the next step (the same instance, or a new one
     * carrying enriched attributes), or `null` to drop it - dropping
     * short-circuits the rest of the pipeline and the entry is never exported.
     */
    public function process(LogEntry $entry): ?LogEntry;
}
