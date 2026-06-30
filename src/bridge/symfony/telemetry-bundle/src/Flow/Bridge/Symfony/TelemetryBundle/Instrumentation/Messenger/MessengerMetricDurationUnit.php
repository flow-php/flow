<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

/**
 * Unit for the messaging.process.duration histogram emitted for consumed messages.
 */
enum MessengerMetricDurationUnit: string
{
    /**
     * Seconds — the unit and bucket boundaries mandated by the OpenTelemetry messaging
     * semantic conventions (https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/).
     */
    case Seconds = 's';

    /**
     * Milliseconds — uses Flow's native default histogram buckets. Diverges from OTEL semconv
     * but matches the rest of Flow's duration histograms.
     */
    case Milliseconds = 'ms';

    /**
     * Explicit histogram bucket boundaries for this unit, or null to use the instrument default.
     *
     * @return list<float>|null
     */
    public function histogramBoundaries(): ?array
    {
        return match ($this) {
            self::Seconds => [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0],
            self::Milliseconds => null,
        };
    }

    /**
     * Convert a duration expressed in milliseconds (Span::duration()) to this unit.
     */
    public function fromMilliseconds(float $milliseconds): float
    {
        return match ($this) {
            self::Seconds => $milliseconds / 1000,
            self::Milliseconds => $milliseconds,
        };
    }
}
