<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Middleware;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogMiddleware;
use Flow\Telemetry\Logger\LogRecord;

/**
 * Merges a fixed set of attributes into every log entry that passes through.
 *
 * The configured attributes act as defaults: an attribute already present on the
 * record (set at the call site) wins over the enricher of the same key. Because
 * {@see LogEntry} and {@see LogRecord} are immutable, enrichment returns a new
 * entry carrying a new record - this is the spec-endorsed "modify the log record"
 * step of a LogRecordProcessor, modelled immutably.
 */
final readonly class EnrichingLogMiddleware implements LogMiddleware
{
    private Attributes $attributes;

    /**
     * @param array<string, array<array-key, mixed>|bool|\DateTimeInterface|float|int|string|\Throwable>|Attributes $attributes
     */
    public function __construct(array|Attributes $attributes)
    {
        $this->attributes = $attributes instanceof Attributes ? $attributes : Attributes::create($attributes);
    }

    public function process(LogEntry $entry): ?LogEntry
    {
        if ($this->attributes->isEmpty()) {
            return $entry;
        }

        return new LogEntry(
            new LogRecord(
                $entry->record->severity,
                $entry->record->body,
                $this->attributes->merge($entry->record->attributes),
                $entry->record->timestamp,
                $entry->record->observedTimestamp,
            ),
            $entry->resource,
            $entry->scope,
            $entry->timestamp,
            $entry->spanContext,
            $entry->droppedAttributeCount,
        );
    }
}
