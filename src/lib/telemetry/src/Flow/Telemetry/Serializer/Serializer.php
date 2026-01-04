<?php

declare(strict_types=1);

namespace Flow\Telemetry\Serializer;

use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Tracer\Span;

/**
 * Interface for telemetry serializers.
 *
 * Implementations convert Flow Telemetry batch objects to wire format
 * for transmission to telemetry backends.
 *
 * Signals carry their own Resource and InstrumentationScope, so serializers
 * must group signals by Resource, then by Scope for the wire format.
 */
interface Serializer
{
    /**
     * Serialize log records batch to wire format.
     *
     * @param array<LogEntry> $entries The log entries to serialize
     *
     * @return string Serialized payload ready for transmission
     */
    public function serializeLogs(array $entries) : string;

    /**
     * Serialize metrics batch to wire format.
     *
     * @param array<Metric> $metrics The metrics to serialize
     *
     * @return string Serialized payload ready for transmission
     */
    public function serializeMetrics(array $metrics) : string;

    /**
     * Serialize spans batch to wire format.
     *
     * @param array<Span> $spans The spans to serialize
     *
     * @return string Serialized payload ready for transmission
     */
    public function serializeSpans(array $spans) : string;
}
