<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

/**
 * Configuration limits for log record data.
 *
 * LogRecordLimits provides bounds on the amount of data a log record can collect,
 * preventing unbounded memory growth and ensuring reasonable log record sizes.
 *
 * Example usage:
 * ```php
 * $limits = new LogRecordLimits(
 *     attributeCountLimit: 64,
 *     attributeValueLengthLimit: 1024,
 * );
 *
 * // Use defaults
 * $defaultLimits = LogRecordLimits::default();
 * ```
 */
final readonly class LogRecordLimits
{
    private const int DEFAULT_ATTRIBUTE_COUNT_LIMIT = 128;

    /**
     * @param int $attributeCountLimit Maximum number of attributes per log record
     * @param null|int $attributeValueLengthLimit Maximum length for string attribute values (null = unlimited)
     */
    public function __construct(
        public int $attributeCountLimit = self::DEFAULT_ATTRIBUTE_COUNT_LIMIT,
        public ?int $attributeValueLengthLimit = null,
    ) {}

    /**
     * Create LogRecordLimits with default values.
     */
    public static function default(): self
    {
        return new self();
    }

    /**
     * Create LogRecordLimits with unlimited values (for development/debugging).
     */
    public static function unlimited(): self
    {
        return new self(attributeCountLimit: PHP_INT_MAX, attributeValueLengthLimit: null);
    }
}
