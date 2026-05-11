<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

/**
 * Configuration for metric cardinality limits.
 *
 * Per OpenTelemetry Metrics SDK specification, metrics need cardinality limits
 * to prevent memory exhaustion from unbounded unique attribute combinations.
 *
 * When the cardinality limit is exceeded, new attribute combinations are
 * redirected to an overflow aggregator identified by the OVERFLOW_ATTRIBUTE.
 *
 * Note: Unlike spans and logs, metrics are EXEMPT from attribute count and
 * value length limits per the OTel specification. Only cardinality is limited.
 *
 * @see https://opentelemetry.io/docs/specs/otel/metrics/sdk/
 */
final readonly class MetricLimits
{
    /**
     * Attribute key used to identify overflow aggregations.
     *
     * When cardinality limit is exceeded, measurements are aggregated
     * into a synthetic data point with this attribute set to true.
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
