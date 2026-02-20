<?php

declare(strict_types=1);

namespace Flow\Telemetry;

/**
 * Result of enforcing limits on Attributes.
 *
 * Contains the resulting attributes after enforcement and the count of
 * attributes that were dropped due to exceeding the limit.
 */
final readonly class EnforcedAttributes
{
    public function __construct(
        public Attributes $attributes,
        public int $droppedAttributeCount,
    ) {
    }
}
