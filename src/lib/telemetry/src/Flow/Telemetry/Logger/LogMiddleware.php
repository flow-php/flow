<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

/**
 * A chainable log-processing step inside a {@see Processor\PipelineLogProcessor}.
 *
 * Unlike a {@see LogProcessor} (which terminates a pipeline by exporting), a
 * middleware transforms or filters a single {@see LogEntry} and hands the result
 * to the next step. It is stateless and has no lifecycle of its own - flush and
 * shutdown belong to the pipeline's {@see LogSink}.
 *
 * This mirrors the OpenTelemetry LogRecordProcessor model, where OnEmit receives a
 * read/write record that a processor "may freely modify" and where filtering is an
 * explicit responsibility. Because {@see LogEntry} is immutable, enrichment returns
 * a new entry rather than mutating in place.
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
