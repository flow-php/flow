<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

/**
 * Configuration limits for span data.
 *
 * SpanLimits provides bounds on the amount of data a span can collect,
 * preventing unbounded memory growth and ensuring reasonable span sizes.
 *
 * Example usage:
 * ```php
 * $limits = new SpanLimits(
 *     attributeCountLimit: 32,
 *     eventCountLimit: 128,
 *     linkCountLimit: 32,
 * );
 *
 * // Use defaults
 * $defaultLimits = SpanLimits::default();
 * ```
 */
final readonly class SpanLimits
{
    private const int DEFAULT_ATTRIBUTE_COUNT_LIMIT = 128;

    private const int DEFAULT_ATTRIBUTE_PER_EVENT_COUNT_LIMIT = 128;

    private const int DEFAULT_ATTRIBUTE_PER_LINK_COUNT_LIMIT = 128;

    private const int DEFAULT_EVENT_COUNT_LIMIT = 128;

    private const int DEFAULT_LINK_COUNT_LIMIT = 128;

    /**
     * @param int $attributeCountLimit Maximum number of attributes per span
     * @param int $eventCountLimit Maximum number of events per span
     * @param int $linkCountLimit Maximum number of links per span
     * @param int $attributePerEventCountLimit Maximum number of attributes per event
     * @param int $attributePerLinkCountLimit Maximum number of attributes per link
     * @param null|int $attributeValueLengthLimit Maximum length for string attribute values (null = unlimited)
     */
    public function __construct(
        public int $attributeCountLimit = self::DEFAULT_ATTRIBUTE_COUNT_LIMIT,
        public int $eventCountLimit = self::DEFAULT_EVENT_COUNT_LIMIT,
        public int $linkCountLimit = self::DEFAULT_LINK_COUNT_LIMIT,
        public int $attributePerEventCountLimit = self::DEFAULT_ATTRIBUTE_PER_EVENT_COUNT_LIMIT,
        public int $attributePerLinkCountLimit = self::DEFAULT_ATTRIBUTE_PER_LINK_COUNT_LIMIT,
        public ?int $attributeValueLengthLimit = null,
    ) {
    }

    /**
     * Create SpanLimits with default values.
     */
    public static function default() : self
    {
        return new self();
    }

    /**
     * Create SpanLimits with unlimited values (for development/debugging).
     */
    public function unlimited() : self
    {
        return new self(
            attributeCountLimit: PHP_INT_MAX,
            eventCountLimit: PHP_INT_MAX,
            linkCountLimit: PHP_INT_MAX,
            attributePerEventCountLimit: PHP_INT_MAX,
            attributePerLinkCountLimit: PHP_INT_MAX,
            attributeValueLengthLimit: null,
        );
    }
}
