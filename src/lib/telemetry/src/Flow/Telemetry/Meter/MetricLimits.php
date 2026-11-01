<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

/**
 * Metrics are exempt from attribute count/length limits (OTel); only cardinality is limited.
 *
 * @see https://opentelemetry.io/docs/specs/otel/metrics/sdk/
 */
final readonly class MetricLimits
{
    /**
     * Set to true on the synthetic data point that absorbs measurements past the cardinality limit.
     */
    public const string OVERFLOW_ATTRIBUTE = 'otel.metric.overflow';

    private const int DEFAULT_CARDINALITY_LIMIT = 2000;

    /**
     * @param int $cardinalityLimit Maximum number of unique attribute combinations per instrument
     */
    public function __construct(
        public int $cardinalityLimit = self::DEFAULT_CARDINALITY_LIMIT,
    ) {}

    /**
     * Create limits with default values.
     */
    public static function default(): self
    {
        return new self();
    }

    /**
     * Create limits with no cardinality restrictions.
     *
     * Warning: This can lead to memory exhaustion with high-cardinality attributes.
     */
    public static function unlimited(): self
    {
        return new self(cardinalityLimit: PHP_INT_MAX);
    }
}
